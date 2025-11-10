<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class WarehouseController extends Controller
{
    /**
     * Mengambil data Warehouse dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncWarehouseData(?string $period = null, ?string $startDate = null): array
    {
        // Inisialisasi dan Konfigurasi
        $INTERNAL_BATCH_SIZE = 500;
        ini_set('memory_limit', '512M');
        set_time_limit(1800); // 30 menit

        $offsetNum = 0;
        $fetchNum = 5000;
        $totalProcessed = 0;
        $batchCount = 0;

        // Definisi Kolom dan Sintaks SQL
        $columnNames = [
            'warehousecode', 'description', 'name', 'address1', 'address2', 'address3',
            'plant', 'warehousetype', 'warehousetypedefault',
            'whproduksi_c', 'whfg_c', 'whincoming_c', 'whng_c', 'whsubcon_c', 'whtype_c',
            'sysrevid', 'sysrowid', 'calculated_changedate'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['warehousecode']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'warehousecode';

        do {
            $apiParams = [
                'OffsetNum' => (string)$offsetNum,
                'FetchNum' => (string)$fetchNum,
                'Periode' => (string)$period,
                'StartDate' => (string)$startDate, // null akan menjadi ""
            ];

            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_Warehse/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data Warehouse", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data Warehouse dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $chunk = collect($chunk)
                    ->reverse()
                    ->unique(fn($r) => $r['Warehse_WarehouseCode'])
                    ->values()
                    ->toArray();
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'Warehse_WarehouseCode'),
                        $getVal($row, 'Warehse_Description'),
                        $getVal($row, 'Warehse_Name'),
                        $getVal($row, 'Warehse_Address1'),
                        $getVal($row, 'Warehse_Address2'),
                        $getVal($row, 'Warehse_Address3'),
                        $getVal($row, 'Warehse_Plant'),
                        $getVal($row, 'Warehse_WarehouseType'),
                        $getBool($row, 'Warehse_WarehouseTypeDefault'),
                        $getBool($row, 'Warehse_whproduksi_c'),
                        $getBool($row, 'Warehse_whfg_c'),
                        $getBool($row, 'Warehse_whincoming_c'),
                        $getBool($row, 'Warehse_whng_c'),
                        $getBool($row, 'Warehse_whsubcon_c'),
                        $getVal($row, 'Warehse_whtype_c'),
                        $getInt($row, 'Warehse_SysRevID'),
                        $getVal($row, 'Warehse_SysRowID'),
                        $getTimestamp($row, 'Calculated_changedate')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO warehse ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("Warehouse Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data Warehouse ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data Warehouse Epicor selesai.',
            'total_processed_api_rows' => $totalProcessed,
            'total_db_batches_processed' => $batchCount,
        ];
    }
    
    /**
     * HTTP Endpoint for fetching data from Epicor API.
     *
     * @param Request $request
     * @return JsonResponse
     */
    public function fetchDataWarehouse(Request $request): JsonResponse
    {
        $result = $this->syncWarehouseData();

        if (!$result['success']) {
            return response()->json([
                'message' => 'Gagal sinkronisasi data Epicor.',
                'error' => $result['error'],
                'details' => $result['details'] ?? null
            ], $result['status_code'] ?? 500);
        }

        // Hapus 'success' dari response agar bersih
        unset($result['success']); 
        
        return response()->json($result, 200);
    }
}

