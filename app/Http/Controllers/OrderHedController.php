<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class OrderHedController extends Controller
{
    /**
     * Mengambil data OrderHed dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncOrderHedData(?string $period = null, ?string $startDate = null): array
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
            'openorder', 'voidorder', 'ordernum', 'custnum', 'ponum',
            'orderheld', 'entryperson', 'shiptonum', 'requestdate', 'orderdate',
            'shipviacode', 'termscode', 'ordercomment', 'needbydate', 'exchangerate',
            'currencycode', 'createinvoice', 'createpackingslip',
            'ccamount', 'ccfreight', 'cctax', 'cctotal',
            'ccdocamount', 'ccdocfreight', 'ccdoctax', 'ccdoctotal',
            'ccstreetaddr', 'cczip', 'btcustnum', 'btconnum', 'refnotes',
            'applychrg', 'chrgamount', 'payflag', 'payaccount',
            'paybtaddress1', 'paybtaddress2', 'paybtcity', 'paybtstate', 'paybtzip',
            'paybtcountry', 'dropship', 'commercialinvoice', 'shipexprtdeclartn',
            'changedby', 'changedate', 'changetime', 'autoprintready', 'deliveryconf',
            'nonstdpkg', 'paybtaddress3', 'paybtcountrynum', 'paybtphone',
            'readytocalc', 'totalcharges', 'totalmisc', 'totaldiscount', 'totalcomm',
            'totaladvbill', 'totallines', 'totalreleases', 'totalreldates',
            'doctotalcharges', 'doctotalmisc', 'doctotaldiscount', 'doctotalcomm',
            'totaltax', 'doctotaltax', 'doctotaladvbill', 'totalshipped',
            'totalinvoiced', 'totalcommlines', 'orderamt', 'docorderamt',
            'taxpoint', 'taxratedate', 'taxregioncode', 'totalwhtax', 'doctotalwhtax',
            'totalsatax', 'doctotalsatax', 'shiptocustnum', 'orderstatus',
            'holdsetbydemand', 'intotalcharges', 'intotalmisc', 'intotaldiscount',
            'docintotalcharges', 'docintotalmisc', 'docintotaldiscount',
            'legalnumber', 'sysrevid', 'sysrowid', 'plant', 'promisedate',
            'finalcustomerid_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['ponum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'ponum';

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
            ->get(env('EPICOR_API_URL'). '/ETL_OrderHed/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data OrderHed", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data OrderHed dari API',
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
                        $getBool($row, 'OrderHed_OpenOrder'), $getBool($row, 'OrderHed_VoidOrder'),
                        $getInt($row, 'OrderHed_OrderNum'), $getInt($row, 'OrderHed_CustNum'),
                        $getVal($row, 'OrderHed_PONum'), $getBool($row, 'OrderHed_OrderHeld'),
                        $getVal($row, 'OrderHed_EntryPerson'), $getVal($row, 'OrderHed_ShipToNum'),
                        $getDate($row, 'OrderHed_RequestDate'), $getDate($row, 'OrderHed_OrderDate'),
                        $getVal($row, 'OrderHed_ShipViaCode'), $getVal($row, 'OrderHed_TermsCode'),
                        $getVal($row, 'OrderHed_OrderComment'), $getDate($row, 'OrderHed_NeedByDate'),
                        $getNum($row, 'OrderHed_ExchangeRate'), $getVal($row, 'OrderHed_CurrencyCode'),
                        $getBool($row, 'OrderHed_CreateInvoice'), $getBool($row, 'OrderHed_CreatePackingSlip'),
                        $getNum($row, 'OrderHed_CCAmount'), $getNum($row, 'OrderHed_CCFreight'),
                        $getNum($row, 'OrderHed_CCTax'), $getNum($row, 'OrderHed_CCTotal'),
                        $getNum($row, 'OrderHed_CCDocAmount'), $getNum($row, 'OrderHed_CCDocFreight'),
                        $getNum($row, 'OrderHed_CCDocTax'), $getNum($row, 'OrderHed_CCDocTotal'),
                        $getVal($row, 'OrderHed_CCStreetAddr'), $getVal($row, 'OrderHed_CCZip'),
                        $getInt($row, 'OrderHed_BTCustNum'), $getInt($row, 'OrderHed_BTConNum'),
                        $getVal($row, 'OrderHed_RefNotes'), $getBool($row, 'OrderHed_ApplyChrg'),
                        $getNum($row, 'OrderHed_ChrgAmount'), $getVal($row, 'OrderHed_PayFlag'),
                        $getVal($row, 'OrderHed_PayAccount'), $getVal($row, 'OrderHed_PayBTAddress1'),
                        $getVal($row, 'OrderHed_PayBTAddress2'), $getVal($row, 'OrderHed_PayBTCity'),
                        $getVal($row, 'OrderHed_PayBTState'), $getVal($row, 'OrderHed_PayBTZip'),
                        $getVal($row, 'OrderHed_PayBTCountry'), $getBool($row, 'OrderHed_DropShip'),
                        $getBool($row, 'OrderHed_CommercialInvoice'), $getBool($row, 'OrderHed_ShipExprtDeclartn'),
                        $getVal($row, 'OrderHed_ChangedBy'), $getDate($row, 'OrderHed_ChangeDate'),
                        $getInt($row, 'OrderHed_ChangeTime'), $getBool($row, 'OrderHed_AutoPrintReady'),
                        $getInt($row, 'OrderHed_DeliveryConf'), $getBool($row, 'OrderHed_NonStdPkg'),
                        $getVal($row, 'OrderHed_PayBTAddress3'), $getInt($row, 'OrderHed_PayBTCountryNum'),
                        $getVal($row, 'OrderHed_PayBTPhone'), $getBool($row, 'OrderHed_ReadyToCalc'),
                        $getNum($row, 'OrderHed_TotalCharges'), $getNum($row, 'OrderHed_TotalMisc'),
                        $getNum($row, 'OrderHed_TotalDiscount'), $getNum($row, 'OrderHed_TotalComm'),
                        $getNum($row, 'OrderHed_TotalAdvBill'), $getInt($row, 'OrderHed_TotalLines'),
                        $getInt($row, 'OrderHed_TotalReleases'), $getInt($row, 'OrderHed_TotalRelDates'),
                        $getNum($row, 'OrderHed_DocTotalCharges'), $getNum($row, 'OrderHed_DocTotalMisc'),
                        $getNum($row, 'OrderHed_DocTotalDiscount'), $getNum($row, 'OrderHed_DocTotalComm'),
                        $getNum($row, 'OrderHed_TotalTax'), $getNum($row, 'OrderHed_DocTotalTax'),
                        $getNum($row, 'OrderHed_DocTotalAdvBill'), $getNum($row, 'OrderHed_TotalShipped'),
                        $getNum($row, 'OrderHed_TotalInvoiced'), $getInt($row, 'OrderHed_TotalCommLines'),
                        $getNum($row, 'OrderHed_OrderAmt'), $getNum($row, 'OrderHed_DocOrderAmt'),
                        $getDate($row, 'OrderHed_TaxPoint'), $getDate($row, 'OrderHed_TaxRateDate'),
                        $getVal($row, 'OrderHed_TaxRegionCode'), $getNum($row, 'OrderHed_TotalWHTax'),
                        $getNum($row, 'OrderHed_DocTotalWHTax'), $getNum($row, 'OrderHed_TotalSATax'),
                        $getNum($row, 'OrderHed_DocTotalSATax'), $getInt($row, 'OrderHed_ShipToCustNum'),
                        $getVal($row, 'OrderHed_OrderStatus'), $getBool($row, 'OrderHed_HoldSetByDemand'),
                        $getNum($row, 'OrderHed_InTotalCharges'), $getNum($row, 'OrderHed_InTotalMisc'),
                        $getNum($row, 'OrderHed_InTotalDiscount'), $getNum($row, 'OrderHed_DocInTotalCharges'),
                        $getNum($row, 'OrderHed_DocInTotalMisc'), $getNum($row, 'OrderHed_DocInTotalDiscount'),
                        $getVal($row, 'OrderHed_LegalNumber'), $getInt($row, 'OrderHed_SysRevID'),
                        $getVal($row, 'OrderHed_SysRowID'), $getVal($row, 'OrderHed_Plant'),
                        $getDate($row, 'OrderHed_PromiseDate'), $getVal($row, 'OrderHed_FinalCustomerID_c'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO orderhed ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("OrderHed Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data OrderHed ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data OrderHed Epicor selesai.',
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
    public function fetchDataOrderHed(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncOrderHedData($period, $startDate);

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

