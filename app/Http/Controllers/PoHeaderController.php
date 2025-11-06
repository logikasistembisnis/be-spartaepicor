<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class PoHeaderController extends Controller
{
    /**
     * Mengambil data PoHeader dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncPoHeaderData(?string $period = null, ?string $startDate = null): array
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
            'openorder', 'voidorder', 'ponum', 'entryperson', 'orderdate', 'shipviacode', 'termscode',
            'shipname', 'shipaddress1', 'shipaddress2', 'shipaddress3', 'shipcity', 'shipstate', 'shipzip',
            'shipcountry', 'buyerid', 'freightpp', 'prcconnum', 'vendornum', 'purpoint', 'commenttext',
            'orderheld', 'shiptoconname', 'readytoprint', 'currencycode', 'exchangerate', 'shipcountrynum',
            'approveddate', 'approvedby', 'approve', 'approvalstatus', 'approvedamount', 'vendorrefnum',
            'confirmreq', 'confirmed', 'confirmvia', 'ordernum', 'legalnumber', 'consolidatedpo',
            'contractorder', 'contractstartdate', 'contractenddate', 'potype', 'trandoctypeid', 'sysrevid',
            'sysrowid', 'duedate', 'promisedate', 'changedby', 'changedate', 'potaxreadytoprocess',
            'taxregioncode', 'taxpoint', 'taxratedate', 'totaltax', 'doctotaltax', 'rpt1totaltax',
            'rpt2totaltax', 'rpt3totaltax', 'totalcharges', 'totalmisccharges', 'totalorder', 'doctotalcharges',
            'doctotalmisc', 'doctotalorder', 'aptaxroundoption', 'carbontotalorder', 'cancelled_c', 'ponumext_c',
            'mscshphdpacknum_c', 'printas'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['ponum', 'vendornum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'ponum, vendornum';

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
            ->get(env('EPICOR_API_URL'). '/ETL_PoHeader/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data PoHeader", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data PoHeader dari API',
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
                    ->unique(fn($r) => $r['POHeader_PONum'] . '-' . $r['POHeader_VendorNum'])
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
                        $getBool($row, 'POHeader_OpenOrder'), $getBool($row, 'POHeader_VoidOrder'), $getInt($row, 'POHeader_PONum'),
                        $getVal($row, 'POHeader_EntryPerson'), $getDate($row, 'POHeader_OrderDate'), 
                        $getVal($row, 'POHeader_ShipViaCode'), $getVal($row, 'POHeader_TermsCode'), 
                        $getVal($row, 'POHeader_ShipName'), $getVal($row, 'POHeader_ShipAddress1'), $getVal($row, 'POHeader_ShipAddress2'), 
                        $getVal($row, 'POHeader_ShipAddress3'), $getVal($row, 'POHeader_ShipCity'), $getVal($row, 'POHeader_ShipState'),
                        $getVal($row, 'POHeader_ShipZIP'), $getVal($row, 'POHeader_ShipCountry'), $getVal($row, 'POHeader_BuyerID'),
                        $getBool($row, 'POHeader_FreightPP'), $getInt($row, 'POHeader_PrcConNum'), $getInt($row, 'POHeader_VendorNum'),
                        $getVal($row, 'POHeader_PurPoint'), $getVal($row, 'POHeader_CommentText'), $getBool($row, 'POHeader_OrderHeld'),
                        $getVal($row, 'POHeader_ShipToConName'), $getBool($row, 'POHeader_ReadyToPrint'), 
                        $getVal($row, 'POHeader_CurrencyCode'), $getNum($row, 'POHeader_ExchangeRate'), 
                        $getInt($row, 'POHeader_ShipCountryNum'), $getDate($row, 'POHeader_ApprovedDate'),
                        $getVal($row, 'POHeader_ApprovedBy'), $getBool($row, 'POHeader_Approve'), $getVal($row, 'POHeader_ApprovalStatus'),
                        $getNum($row, 'POHeader_ApprovedAmount'), $getVal($row, 'POHeader_VendorRefNum'), $getBool($row, 'POHeader_ConfirmReq'),
                        $getBool($row, 'POHeader_Confirmed'), $getVal($row, 'POHeader_ConfirmVia'),
                        $getInt($row, 'POHeader_OrderNum'), $getVal($row, 'POHeader_LegalNumber'), $getBool($row, 'POHeader_ConsolidatedPO'),
                        $getBool($row, 'POHeader_ContractOrder'), $getDate($row, 'POHeader_ContractStartDate'), $getDate($row, 'POHeader_ContractEndDate'),
                        $getVal($row, 'POHeader_POType'), $getVal($row, 'POHeader_TranDocTypeID'), $getInt($row, 'POHeader_SysRevID'),
                        $getVal($row, 'POHeader_SysRowID'), $getDate($row, 'POHeader_DueDate'), $getDate($row, 'POHeader_PromiseDate'),
                        $getVal($row, 'POHeader_ChangedBy'), $getTimestamp($row, 'POHeader_ChangeDate'), $getBool($row, 'POHeader_POTaxReadyToProcess'),
                        $getVal($row, 'POHeader_TaxRegionCode'), $getDate($row, 'POHeader_TaxPoint'), $getDate($row, 'POHeader_TaxRateDate'),
                        $getNum($row, 'POHeader_TotalTax'), $getNum($row, 'POHeader_DocTotalTax'), $getNum($row, 'POHeader_Rpt1TotalTax'),
                        $getNum($row, 'POHeader_Rpt2TotalTax'), $getNum($row, 'POHeader_Rpt3TotalTax'), $getNum($row, 'POHeader_TotalCharges'),
                        $getNum($row, 'POHeader_TotalMiscCharges'), $getNum($row, 'POHeader_TotalOrder'), $getNum($row, 'POHeader_DocTotalCharges'),
                        $getNum($row, 'POHeader_DocTotalMisc'), $getNum($row, 'POHeader_DocTotalOrder'), $getInt($row, 'POHeader_APTaxRoundOption'),
                        $getNum($row, 'POHeader_CarbonTotalOrder'), $getBool($row, 'POHeader_Cancelled_c'), 
                        $getVal($row, 'POHeader_PONumExt_c'), $getInt($row, 'POHeader_MscShpHdPackNum_c'), 
                        $getVal($row, 'POHeader_PrintAs')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO poheader ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("PoHeader Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data PoHeader ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data PoHeader Epicor selesai.',
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
    public function fetchDataPoHeader(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncPoHeaderData($period, $startDate);

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

