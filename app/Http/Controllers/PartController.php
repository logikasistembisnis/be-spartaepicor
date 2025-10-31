<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class PartController extends Controller
{
    /**
     * Mengambil data Part dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncPartData(?string $period = null, ?string $startDate = null): array
    {
        // Inisialisasi dan Konfigurasi
        $INTERNAL_BATCH_SIZE = 500;
        ini_set('memory_limit', '512M');
        set_time_limit(1800); // 30 menit

        if (is_null($startDate)) {
            $startDate = date('Ymd');
        }
        if (is_null($period)) {
            $period = date('ym', strtotime($startDate));
        }

        $offsetNum = 0;
        $fetchNum = 5000;
        $totalProcessed = 0;
        $batchCount = 0;

        // Definisi Kolom dan Sintaks SQL
        $columnNames = [
            'partnum', 'searchword', 'partdescription', 'classid', 'ium', 'pum',
            'typecode', 'nonstock', 'purchasingfactor', 'unitprice', 'pricepercode',
            'internalunitprice', 'internalpricepercode', 'prodcode', 'mfgcomment', 'purcomment',
            'costmethod', 'inactive', 'method', 'tracklots', 'commoditycode',
            'warrantycode', 'salesum', 'sellingfactor', 'mtlburrate', 'netweight',
            'usepartrev', 'partspercontainer', 'partlength', 'partwidth', 'partheight',
            'analysiscode', 'globalpart', 'consolidatedpurchasing', 'purchasingfactordirection',
            'sellingfactordirection', 'qtybearing', 'uomclassid', 'buytoorder', 'dropship',
            'grossweight', 'grossweightuom', 'basepartnum', 'durometer', 'specification',
            'commercialbrand', 'commercialsubbrand', 'commercialcategory', 'commercialsubcategory',
            'commercialstyle', 'commercialsize1', 'commercialsize2', 'commercialcolor',
            'photofile', 'partphotoexists', 'commenttext', 'imageid', 'createdby', 'createdon',
            'changedon', 'saleable', 'partfg_c', 'heijunka_c', 'supplierpartnum_c',
            'supplierpartdesc_c', 'lineke_c', 'grprptmcp_c', 'grprptmcpsc_c', 'custsupply_c',
            'partnumparent_c', 'krwtype_c', 'dyhtype_c', 'srgtype_c','sysrevid', 'sysrowid'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['partnum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'partnum';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_Part/Data', [
                'Periode' => $period,
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum,
                'StartDate' => $startDate,
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data Part", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data Part dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';

                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'Part_PartNum'), $getVal($row, 'Part_SearchWord'), $getVal($row, 'Part_PartDescription'),
                        $getVal($row, 'Part_ClassID'), $getVal($row, 'Part_IUM'), $getVal($row, 'Part_PUM'),
                        $getVal($row, 'Part_TypeCode'), $getBool($row, 'Part_NonStock'),
                        $getNum($row, 'Part_PurchasingFactor'), $getNum($row, 'Part_UnitPrice'), $getVal($row, 'Part_PricePerCode'),
                        $getNum($row, 'Part_InternalUnitPrice'), $getVal($row, 'Part_InternalPricePerCode'),
                        $getVal($row, 'Part_ProdCode'), $getVal($row, 'Part_MfgComment'), $getVal($row, 'Part_PurComment'),
                        $getVal($row, 'Part_CostMethod'), $getBool($row, 'Part_InActive'), $getBool($row, 'Part_Method'),
                        $getBool($row, 'Part_TrackLots'), $getVal($row, 'Part_CommodityCode'), $getVal($row, 'Part_WarrantyCode'),
                        $getVal($row, 'Part_SalesUM'), $getNum($row, 'Part_SellingFactor'), $getNum($row, 'Part_MtlBurRate'),
                        $getNum($row, 'Part_NetWeight'), $getBool($row, 'Part_UsePartRev'), $getInt($row, 'Part_PartsPerContainer'),
                        $getNum($row, 'Part_PartLength'), $getNum($row, 'Part_PartWidth'), $getNum($row, 'Part_PartHeight'),
                        $getVal($row, 'Part_AnalysisCode'), $getBool($row, 'Part_GlobalPart'), $getBool($row, 'Part_ConsolidatedPurchasing'),
                        $getVal($row, 'Part_PurchasingFactorDirection'), $getVal($row, 'Part_SellingFactorDirection'),
                        $getBool($row, 'Part_QtyBearing'), $getVal($row, 'Part_UOMClassID'), $getBool($row, 'Part_BuyToOrder'),
                        $getBool($row, 'Part_DropShip'), $getNum($row, 'Part_GrossWeight'), $getVal($row, 'Part_GrossWeightUOM'),
                        $getVal($row, 'Part_BasePartNum'), $getVal($row, 'Part_Durometer'), $getVal($row, 'Part_Specification'),
                        $getVal($row, 'Part_CommercialBrand'), $getVal($row, 'Part_CommercialSubBrand'),
                        $getVal($row, 'Part_CommercialCategory'), $getVal($row, 'Part_CommercialSubCategory'),
                        $getVal($row, 'Part_CommercialStyle'), $getVal($row, 'Part_CommercialSize1'),
                        $getVal($row, 'Part_CommercialSize2'), $getVal($row, 'Part_CommercialColor'),
                        $getVal($row, 'Part_PhotoFile'), $getBool($row, 'Part_PartPhotoExists'), $getVal($row, 'Part_CommentText'),
                        $getVal($row, 'Part_ImageID'), $getVal($row, 'Part_CreatedBy'),
                        $getTimestamp($row, 'Part_CreatedOn'), $getTimestamp($row, 'Part_ChangedOn'),
                        $getVal($row, 'Part_Saleable'), $getVal($row, 'Part_PartFG_c'), $getNum($row, 'Part_Heijunka_c'),
                        $getVal($row, 'Part_SupplierPartNum_c'), $getVal($row, 'Part_SupplierPartDesc_c'),
                        $getInt($row, 'Part_lineke_c'), $getVal($row, 'Part_GrpRptMCP_c'), $getVal($row, 'Part_GrpRptMCPSC_c'),
                        $getBool($row, 'Part_CustSupply_c'), $getVal($row, 'Part_PartNumParent_c'),
                        $getVal($row, 'Part_KrwType_c'), $getVal($row, 'Part_DyhType_c'), $getVal($row, 'Part_SrgType_c'),
                        $getInt($row, 'Part_SysRevID'), $getVal($row, 'Part_SysRowID'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO part ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("Part Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data Part ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data Part Epicor selesai.',
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
    public function fetchDataPart(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncPartData($period, $startDate);

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

