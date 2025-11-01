<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class PoDetailController extends Controller
{
    /**
     * Mengambil data PoDetail dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncPoDetailData(?string $period = null, ?string $startDate = null): array
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
            'openline', 'voidline', 'ponum', 'poline', 'linedesc', 'ium', 'unitcost', 'docunitcost', 'orderqty',
            'xorderqty', 'taxable', 'pum', 'costpercode', 'partnum', 'venpartnum', 'commenttext', 'classid', 
            'revisionnum', 'rcvinspectionreq', 'vendornum', 'advancepaybal', 'docadvancepaybal', 'confirmed', 
            'datechgreq', 'qtychgreq', 'ordernum', 'orderline', 'baseqty', 'baseuom', 'btoordernum', 'btoorderline',
            'vendorpartopts', 'mfgpartopts', 'subpartopts', 'convoverride', 'uom', 'sysrevid', 'sysrowid', 'groupseq',
            'qtyoption', 'duedate', 'changedby', 'changedate', 'taxcatid', 'docextcost', 'extcost', 'docmisccost',
            'misccost', 'totaltax', 'doctotaltax', 'carbonextcost', 'assetpurchase', 'assetnum', 'itlegalnum_c',
            'pricebeforediscount_c', 'discountamount_c', 'closeqtyots_c', 'qtyporeduction_c', 'reasonqtyreduction_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['ponum', 'poline']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'ponum, poline';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_PoDetail/Data', [
                'Periode' => $period,
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum,
                'StartDate' => $startDate,
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data PoDetail", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data PoDetail dari API',
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
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';

                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getBool($row, 'PODetail_OpenLine'), $getBool($row, 'PODetail_VoidLine'), 
                        $getInt($row, 'PODetail_PONUM'), $getInt($row, 'PODetail_POLine'), 
                        $getVal($row, 'PODetail_LineDesc'), $getVal($row, 'PODetail_IUM'), 
                        $getNum($row, 'PODetail_UnitCost'), $getNum($row, 'PODetail_DocUnitCost'), 
                        $getNum($row, 'PODetail_OrderQty'), $getNum($row, 'PODetail_XOrderQty'), 
                        $getBool($row, 'PODetail_Taxable'), $getVal($row, 'PODetail_PUM'), 
                        $getVal($row, 'PODetail_CostPerCode'), $getVal($row, 'PODetail_PartNum'), 
                        $getVal($row, 'PODetail_VenPartNum'), $getVal($row, 'PODetail_CommentText'),
                        $getVal($row, 'PODetail_ClassID'), $getVal($row, 'PODetail_RevisionNum'), 
                        $getBool($row, 'PODetail_RcvInspectionReq'), $getInt($row, 'PODetail_VendorNum'), 
                        $getNum($row, 'PODetail_AdvancePayBal'), $getNum($row, 'PODetail_DocAdvancePayBal'),
                        $getBool($row, 'PODetail_Confirmed'), $getBool($row, 'PODetail_DateChgReq'), 
                        $getBool($row, 'PODetail_QtyChgReq'), $getInt($row, 'PODetail_OrderNum'), 
                        $getInt($row, 'PODetail_OrderLine'), $getNum($row, 'PODetail_BaseQty'),
                        $getVal($row, 'PODetail_BaseUOM'), $getInt($row, 'PODetail_BTOOrderNum'), 
                        $getInt($row, 'PODetail_BTOOrderLine'), $getVal($row, 'PODetail_VendorPartOpts'), 
                        $getVal($row, 'PODetail_MfgPartOpts'), $getVal($row, 'PODetail_SubPartOpts'),
                        $getBool($row, 'PODetail_ConvOverRide'), $getVal($row, 'PODetail_UOM'),
                        $getInt($row, 'PODetail_SysRevID'), $getVal($row, 'PODetail_SysRowID'), 
                        $getInt($row, 'PODetail_GroupSeq'), $getVal($row, 'PODetail_QtyOption'), 
                        $getDate($row, 'PODetail_DueDate'), $getVal($row, 'PODetail_ChangedBy'),
                        $getTimestamp($row, 'PODetail_ChangeDate'), $getVal($row, 'PODetail_TaxCatID'), 
                        $getNum($row, 'PODetail_DocExtCost'), $getNum($row, 'PODetail_ExtCost'), 
                        $getNum($row, 'PODetail_DocMiscCost'), $getNum($row, 'PODetail_MiscCost'),
                        $getNum($row, 'PODetail_TotalTax'), $getNum($row, 'PODetail_DocTotalTax'),
                        $getNum($row, 'PODetail_CarbonExtCost'), $getBool($row, 'PODetail_AssetPurchase'), 
                        $getVal($row, 'PODetail_AssetNum'), $getVal($row, 'PODetail_ITLegalNum_c'),
                        $getNum($row, 'PODetail_pricebeforediscount_c'), $getNum($row, 'PODetail_discountamount_c'), 
                        $getBool($row, 'PODetail_CloseQtyOts_c'), $getNum($row, 'PODetail_QtyPOReduction_c'), 
                        $getVal($row, 'PODetail_ReasonQtyReduction_c')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO podetail ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("PoDetail Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data PoDetail ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data PoDetail Epicor selesai.',
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
    public function fetchDataPoDetail(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncPoDetailData($period, $startDate);

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

