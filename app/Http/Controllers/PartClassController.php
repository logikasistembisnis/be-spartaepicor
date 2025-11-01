<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class PartClassController extends Controller
{
    /**
     * Mengambil data PartClass dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncPartClassData(): array
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
            'classid', 'description', 'buyerid', 'rcvinspectionreq', 'commoditycode', 'availforreq', 'mtlburrate',
            'splitpoline', 'negqtyaction', 'purchcode', 'consolidatedpurchasing', 'globalpartclass', 'globallock', 
            'sysrevid', 'sysrowid', 'mmsinclude', 'partleadtime', 'inactive', 'spartainspection_c', 
            'groupclass_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['classid']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'classid';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_PartClass/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data PartClass", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data PartClass dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'PartClass_ClassID'), $getVal($row, 'PartClass_Description'),
                        $getVal($row, 'PartClass_BuyerID'), $getBool($row, 'PartClass_RcvInspectionReq'),
                        $getVal($row, 'PartClass_CommodityCode'), $getBool($row, 'PartClass_AvailForReq'),
                        $getNum($row, 'PartClass_MtlBurRate'), $getBool($row, 'PartClass_SplitPOLine'),
                        $getVal($row, 'PartClass_NegQtyAction'), $getVal($row, 'PartClass_PurchCode'),
                        $getBool($row, 'PartClass_ConsolidatedPurchasing'), $getBool($row, 'PartClass_GlobalPartClass'),
                        $getBool($row, 'PartClass_GlobalLock'), $getInt($row, 'PartClass_SysRevID'),
                        $getVal($row, 'PartClass_SysRowID'), $getBool($row, 'PartClass_MMSInclude'),
                        $getInt($row, 'PartClass_PartLeadTime'), $getBool($row, 'PartClass_Inactive'), 
                        $getBool($row, 'PartClass_SpartaInspection_c'), $getVal($row, 'PartClass_GroupClass_c')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO partclass ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("PartClass Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data PartClass ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data PartClass Epicor selesai.',
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
    public function fetchDataPartClass(Request $request): JsonResponse
    {
        $result = $this->syncPartClassData();

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

