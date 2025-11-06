<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class RcvHeadController extends Controller
{
    /**
     * Mengambil data RcvHead dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncRcvHeadData(?string $period = null, ?string $startDate = null): array
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
            'vendornum', 'purpoint', 'packslip', 'receiptdate', 'entryperson', 'saveforinvoicing', 'invoiced', 
            'receiptcomment', 'receiveperson', 'shipviacode', 'entrydate', 'plant', 'ponum', 'landedcost', 'legalnumber',
            'lcvariance', 'containerid', 'lcdisbursemethod', 'potype', 'lcdutyamt', 'lcindcost',
            'applytolc', 'received', 'arriveddate', 'appliedrcptlcamt', 'lcupliftindcost',  'appliedlcvariance',
            'trandoctypeid', 'importnum', 'importedfrom', 'importedfromdesc', 'grossweight', 'grossweightuom',
            'sysrevid', 'sysrowid', 'changedby', 'changedate', 'taxregioncode', 'taxpoint', 'taxratedate', 'inprice'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['vendornum', 'packslip', 'ponum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'vendornum, packslip, ponum';

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
            ->get(env('EPICOR_API_URL'). '/ETL_RcvHead/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data RcvHead", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data RcvHead dari API',
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
                    ->unique(fn($r) => $r['RcvHead_VendorNum'] . '-' . $r['RcvHead_PackSlip'] . '-' . $r['RcvHead_PONum'])
                    ->values()
                    ->toArray();

                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';

                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getInt($row, 'RcvHead_VendorNum'), $getVal($row, 'RcvHead_PurPoint'), $getVal($row, 'RcvHead_PackSlip'),
                        $getDate($row, 'RcvHead_ReceiptDate'), $getVal($row, 'RcvHead_EntryPerson'), 
                        $getBool($row, 'RcvHead_SaveForInvoicing'), $getBool($row, 'RcvHead_Invoiced'), 
                        $getVal($row, 'RcvHead_ReceiptComment'), $getVal($row, 'RcvHead_ReceivePerson'), $getVal($row, 'RcvHead_ShipViaCode'), 
                        $getDate($row, 'RcvHead_EntryDate'), $getVal($row, 'RcvHead_Plant'), $getInt($row, 'RcvHead_PONum'),
                        $getNum($row, 'RcvHead_LandedCost'), $getVal($row, 'RcvHead_LegalNumber'), $getNum($row, 'RcvHead_LCVariance'),
                        $getInt($row, 'RcvHead_ContainerID'), $getVal($row, 'RcvHead_LCDisburseMethod'), $getVal($row, 'RcvHead_POType'),
                        $getNum($row, 'RcvHead_LCDutyAmt'), $getInt($row, 'RcvHead_LCIndCost'), $getBool($row, 'RcvHead_ApplyToLC'),
                        $getBool($row, 'RcvHead_Received'), $getDate($row, 'RcvHead_ArrivedDate'), $getNum($row, 'RcvHead_AppliedRcptLCAmt'),
                        $getNum($row, 'RcvHead_LCUpliftIndCost'), $getNum($row, 'RcvHead_AppliedLCVariance'), $getVal($row, 'RcvHead_TranDocTypeID'),
                        $getVal($row, 'RcvHead_ImportNum'), $getInt($row, 'RcvHead_ImportedFrom'), $getVal($row, 'RcvHead_ImportedFromDesc'),
                        $getNum($row, 'RcvHead_GrossWeight'), $getVal($row, 'RcvHead_GrossWeightUOM'), $getInt($row, 'RcvHead_SysRevID'),
                        $getVal($row, 'RcvHead_SysRowID'), $getVal($row, 'RcvHead_ChangedBy'),
                        $getTimestamp($row, 'RcvHead_ChangeDate'), $getVal($row, 'RcvHead_TaxRegionCode'), $getDate($row, 'RcvHead_TaxPoint'),
                        $getDate($row, 'RcvHead_TaxRateDate'), $getBool($row, 'RcvHead_InPrice')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO rcvhead ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("RcvHead Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data RcvHead ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data RcvHead Epicor selesai.',
            'filter_start_date' => $startDate,
            'filter_period' => $period,
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
    public function fetchDataRcvHead(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncRcvHeadData($period, $startDate);

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

