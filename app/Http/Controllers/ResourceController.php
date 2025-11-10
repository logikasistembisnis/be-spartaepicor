<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class ResourceController extends Controller
{
    /**
     * Mengambil data Resource dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncResourceData(?string $period = null, ?string $startDate = null): array
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
            'resourcegrpid', 'resourceid', 'description', 'inactive', 'finite', 'allowmanualoverride', 'nextmaintdate',
            'outputwhse', 'outputbinnum', 'backflushwhse', 'backflushbinnum', 'inputwhse', 'inputbinnum', 'resourcetype', 
            'concurrentcapacity', 'trackprodqty', 'assetnum', 'prodburrate', 'prodlabrate', 'setupburrate', 'setuplabrate', 
            'qprodburrate', 'qprodlabrate', 'qsetupburrate', 'qsetuplabrate', 'qburdentype', 'vendornum', 'burdentype', 
            'calendarid', 'movehours', 'quehours', 'opcode', 'opstdid', 'splitoperations', 'dailyprodqty', 'billlaborrate', 
            'dailyprodrate', 'location', 'inspplanpartnum', 'specid', 'lastcaldate', 'inspplanrevnum', 'specrevnum', 'equipid', 
            'setuptime', 'sysrevid', 'sysrowid', 'calculated_changedate'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['resourcegrpid', 'resourceid']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'resourcegrpid, resourceid';

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
            ->get(env('EPICOR_API_URL'). '/ETL_Resource/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data Resource", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data Resource dari API',
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
                ->unique(fn($r) => $r['Resource_ResourceGrpID'] . '-' . $r['Resource_ResourceID'])
                ->values()
                ->toArray();

                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'Resource_ResourceGrpID'), $getVal($row, 'Resource_ResourceID'),
                        $getVal($row, 'Resource_Description'), $getBool($row, 'Resource_Inactive'),
                        $getBool($row, 'Resource_Finite'), $getBool($row, 'Resource_AllowManualOverride'),
                        $getDate($row, 'Resource_NextMaintDate'),
                        $getVal($row, 'Resource_OutputWhse'), $getVal($row, 'Resource_OutputBinNum'),
                        $getVal($row, 'Resource_BackflushWhse'), $getVal($row, 'Resource_BackflushBinNum'),
                        $getVal($row, 'Resource_InputWhse'), $getVal($row, 'Resource_InputBinNum'),
                        $getVal($row, 'Resource_ResourceType'),
                        $getNum($row, 'Resource_ConcurrentCapacity'), $getBool($row, 'Resource_TrackProdQty'),
                        $getVal($row, 'Resource_AssetNum'), $getNum($row, 'Resource_ProdBurRate'),
                        $getNum($row, 'Resource_ProdLabRate'), $getNum($row, 'Resource_SetupBurRate'),
                        $getNum($row, 'Resource_SetupLabRate'), $getNum($row, 'Resource_QProdBurRate'),
                        $getNum($row, 'Resource_QProdLabRate'), $getNum($row, 'Resource_QSetupBurRate'),
                        $getNum($row, 'Resource_QSetupLabRate'), $getVal($row, 'Resource_QBurdenType'),
                        $getInt($row, 'Resource_VendorNum'), $getVal($row, 'Resource_BurdenType'),
                        $getVal($row, 'Resource_CalendarID'), $getNum($row, 'Resource_MoveHours'),
                        $getNum($row, 'Resource_QueHours'), $getVal($row, 'Resource_OpCode'),
                        $getVal($row, 'Resource_OpStdID'), $getBool($row, 'Resource_SplitOperations'),
                        $getNum($row, 'Resource_DailyProdQty'), $getNum($row, 'Resource_BillLaborRate'),
                        $getNum($row, 'Resource_DailyProdRate'), $getBool($row, 'Resource_Location'),
                        $getVal($row, 'Resource_InspPlanPartNum'), $getVal($row, 'Resource_SpecID'),
                        $getDate($row, 'Resource_LastCalDate'), $getVal($row, 'Resource_InspPlanRevNum'),
                        $getVal($row, 'Resource_SpecRevNum'), $getVal($row, 'Resource_EquipID'),
                        $getInt($row, 'Resource_SetupTime'), $getInt($row, 'Resource_SysRevID'),
                        $getVal($row, 'Resource_SysRowID'), $getTimestamp($row, 'Calculated_changedate')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO resource ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("Resource Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data Resource ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data Resource Epicor selesai.',
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
    public function fetchDataResource(Request $request): JsonResponse
    {
        $result = $this->syncResourceData();

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

