<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class OrderDtlController extends Controller
{
    /**
     * Mengambil data OrderDtl dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @param string|null $period Periode bulan (e.g., '2510').
     * @param string|null $startDate Tanggal mulai (YYYYMMDD).
     * @return array Hasil summary
     */
    public function syncOrderDtlData(?string $period = null, ?string $startDate = null): array
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
            'voidline', 'openline', 'ordernum', 'orderline', 'linetype', 'partnum', 'linedesc',
            'reference', 'ium', 'revisionnum', 'poline', 'unitprice', 'docunitprice', 'orderqty',
            'discount', 'docdiscount', 'requestdate', 'prodcode', 'xpartnum', 'xrevisionnum',
            'pricepercode', 'ordercomment', 'taxcatid', 'advancebillbal', 'docadvancebillbal',
            'quotenum', 'quoteline', 'needbydate', 'custnum', 'rework', 'rmanum', 'rmaline',
            'contractnum', 'contractcode', 'basepartnum', 'warranty', 'warrantycode',
            'materialduration', 'laborduration', 'miscduration', 'materialmod', 'labormod',
            'salesum', 'sellingfactor', 'sellingquantity', 'salescatid', 'shiplinecomplete',
            'overridepricelist', 'baserevisionnum', 'pricingvalue', 'displayseq',
            'sellingfactordirection', 'demandcontractline', 'createnewjob', 'schedjob', 'reljob',
            'changedby', 'changedate', 'changetime', 'reversecharge', 'totalreleases',
            'extpricedtl', 'docextpricedtl', 'linestatus', 'inunitprice', 'docinunitprice',
            'indiscount', 'docindiscount', 'inlistprice', 'docinlistprice', 'inordbasedprice',
            'docinordbasedprice', 'inextpricedtl', 'docinextpricedtl', 'oldouropenqty',
            'oldsellingopenqty', 'oldopenvalue', 'oldprodcode', 'prevsellqty', 'prevpartnum',
            'prevxpartnum', 'disclistprice', 'sysrevid', 'sysrowid', 'docinadvancebillbal',
            'inadvancebillbal', 'commoditycode', 'endcustomerprice', 'docendcustomerprice',
            'attributesetid', 'promisedate', 'qtypoori_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['ordernum', 'orderline']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'ordernum, orderline';

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
            ->get(env('EPICOR_API_URL'). '/ETL_OrderDtl/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data OrderDtl", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data OrderDtl dari API',
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
                ->unique(fn($r) => $r['OrderDtl_OrderNum'] . '-' . $r['OrderDtl_OrderLine'])
                ->values()
                ->toArray();
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getFloat = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getBool($row, 'OrderDtl_VoidLine'), $getBool($row, 'OrderDtl_OpenLine'),
                        $getInt($row, 'OrderDtl_OrderNum'), $getInt($row, 'OrderDtl_OrderLine'),
                        $getVal($row, 'OrderDtl_LineType'), $getVal($row, 'OrderDtl_PartNum'),
                        $getVal($row, 'OrderDtl_LineDesc'), $getVal($row, 'OrderDtl_Reference'),
                        $getVal($row, 'OrderDtl_IUM'), $getVal($row, 'OrderDtl_RevisionNum'),
                        $getVal($row, 'OrderDtl_POLine'), $getFloat($row, 'OrderDtl_UnitPrice'),
                        $getFloat($row, 'OrderDtl_DocUnitPrice'), $getFloat($row, 'OrderDtl_OrderQty'),
                        $getFloat($row, 'OrderDtl_Discount'), $getFloat($row, 'OrderDtl_DocDiscount'),
                        $getDate($row, 'OrderDtl_RequestDate'), $getVal($row, 'OrderDtl_ProdCode'),
                        $getVal($row, 'OrderDtl_XPartNum'), $getVal($row, 'OrderDtl_XRevisionNum'),
                        $getVal($row, 'OrderDtl_PricePerCode'), $getVal($row, 'OrderDtl_OrderComment'),
                        $getVal($row, 'OrderDtl_TaxCatID'), $getFloat($row, 'OrderDtl_AdvanceBillBal'),
                        $getFloat($row, 'OrderDtl_DocAdvanceBillBal'), $getInt($row, 'OrderDtl_QuoteNum'),
                        $getInt($row, 'OrderDtl_QuoteLine'), $getDate($row, 'OrderDtl_NeedByDate'),
                        $getInt($row, 'OrderDtl_CustNum'), $getBool($row, 'OrderDtl_Rework'),
                        $getInt($row, 'OrderDtl_RMANum'), $getInt($row, 'OrderDtl_RMALine'),
                        $getInt($row, 'OrderDtl_ContractNum'), $getVal($row, 'OrderDtl_ContractCode'),
                        $getVal($row, 'OrderDtl_BasePartNum'), $getBool($row, 'OrderDtl_Warranty'),
                        $getVal($row, 'OrderDtl_WarrantyCode'), $getInt($row, 'OrderDtl_MaterialDuration'),
                        $getInt($row, 'OrderDtl_LaborDuration'), $getInt($row, 'OrderDtl_MiscDuration'),
                        $getVal($row, 'OrderDtl_MaterialMod'), $getVal($row, 'OrderDtl_LaborMod'),
                        $getVal($row, 'OrderDtl_SalesUM'), $getFloat($row, 'OrderDtl_SellingFactor'),
                        $getFloat($row, 'OrderDtl_SellingQuantity'), $getVal($row, 'OrderDtl_SalesCatID'),
                        $getBool($row, 'OrderDtl_ShipLineComplete'), $getBool($row, 'OrderDtl_OverridePriceList'),
                        $getVal($row, 'OrderDtl_BaseRevisionNum'), $getFloat($row, 'OrderDtl_PricingValue'),
                        $getFloat($row, 'OrderDtl_DisplaySeq'), $getVal($row, 'OrderDtl_SellingFactorDirection'),
                        $getInt($row, 'OrderDtl_DemandContractLine'), $getBool($row, 'OrderDtl_CreateNewJob'),
                        $getBool($row, 'OrderDtl_SchedJob'), $getBool($row, 'OrderDtl_RelJob'),
                        $getVal($row, 'OrderDtl_ChangedBy'), $getDate($row, 'OrderDtl_ChangeDate'),
                        $getInt($row, 'OrderDtl_ChangeTime'), $getBool($row, 'OrderDtl_ReverseCharge'),
                        $getInt($row, 'OrderDtl_TotalReleases'), $getFloat($row, 'OrderDtl_ExtPriceDtl'),
                        $getFloat($row, 'OrderDtl_DocExtPriceDtl'), $getVal($row, 'OrderDtl_LineStatus'),
                        $getFloat($row, 'OrderDtl_InUnitPrice'), $getFloat($row, 'OrderDtl_DocInUnitPrice'),
                        $getFloat($row, 'OrderDtl_InDiscount'), $getFloat($row, 'OrderDtl_DocInDiscount'),
                        $getFloat($row, 'OrderDtl_InListPrice'), $getFloat($row, 'OrderDtl_DocInListPrice'),
                        $getFloat($row, 'OrderDtl_InOrdBasedPrice'), $getFloat($row, 'OrderDtl_DocInOrdBasedPrice'),
                        $getFloat($row, 'OrderDtl_InExtPriceDtl'), $getFloat($row, 'OrderDtl_DocInExtPriceDtl'),
                        $getFloat($row, 'OrderDtl_OldOurOpenQty'), $getFloat($row, 'OrderDtl_OldSellingOpenQty'),
                        $getFloat($row, 'OrderDtl_OldOpenValue'), $getVal($row, 'OrderDtl_OldProdCode'),
                        $getFloat($row, 'OrderDtl_PrevSellQty'), $getVal($row, 'OrderDtl_PrevPartNum'),
                        $getVal($row, 'OrderDtl_PrevXPartNum'), $getFloat($row, 'OrderDtl_DiscListPrice'),
                        $getInt($row, 'OrderDtl_SysRevID'), $getVal($row, 'OrderDtl_SysRowID'),
                        $getFloat($row, 'OrderDtl_DocInAdvanceBillBal'), $getFloat($row, 'OrderDtl_InAdvanceBillBal'),
                        $getVal($row, 'OrderDtl_CommodityCode'), $getFloat($row, 'OrderDtl_EndCustomerPrice'),
                        $getFloat($row, 'OrderDtl_DocEndCustomerPrice'), $getInt($row, 'OrderDtl_AttributeSetID'),
                        $getDate($row, 'OrderDtl_PromiseDate'), $getFloat($row, 'OrderDtl_qtypoori_c'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO orderdtl ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("OrderDtl Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data OrderDtl ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data OrderDtl Epicor selesai.',
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
    public function fetchDataOrderDtl(Request $request): JsonResponse
    {
        // Ambil parameter dari query string (bisa null)
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        // Panggil fungsi inti. 
        $result = $this->syncOrderDtlData($period, $startDate);

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

