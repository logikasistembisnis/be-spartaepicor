<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class JobMtlController extends Controller
{
    /**
     * Mengambil data JobMtl dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncJobMtlData(): array
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
            'jobcomplete', 'issuedcomplete', 'jobnum', 'assemblyseq', 'mtlseq', 'partnum', 'description', 'qtyper', 
            'requiredqty', 'ium', 'leadtime', 'relatedoperation', 'mtlburrate', 'estmtlburunitcost', 'estunitcost', 
            'issuedqty', 'totalcost', 'mtlburcost', 'reqdate', 'warehousecode', 'salvagepartnum', 'salvagedescription',
            'salvageqtyper', 'salvageum', 'salvagemtlburrate', 'salvageunitcredit', 'salvageestmtlburunitcredit', 
            'salvageqtytodate', 'salvagecredit', 'salvagemtlburcredit', 'mfgcomment', 'vendornum', 'purpoint', 'buyit',
            'ordered', 'purcomment', 'backflush', 'estscrap', 'estscraptype', 'fixedqty', 'findnum', 'revisionnum',
            'sndalrtcmpl', 'rcvinspectionreq', 'plant', 'direct', 'materialmtlcost', 'materiallabcost', 'materialsubcost',
            'materialburcost', 'salvagemtlcredit', 'salvagelbrcredit', 'salvageburcredit', 'salvagesubcredit', 'apsaddrestype', 
            'callnum', 'callline', 'prodcode', 'unitprice', 'billableunitprice', 'docbillableunitprice', 'resreasoncode',
            'pricepercode', 'billable', 'shippedqty', 'docunitprice', 'qtystagedtodate', 'addedmtl', 'rfqneeded', 'rfqvendquotes', 
            'rfqnum', 'rfqline', 'rpt1billableunitprice', 'baserequiredqty', 'baseuom', 'weight', 'weightuom', 'reqrefdes',
            'basepartnum', 'baserevisionnum', 'selectforpicking', 'estmtlunitcost', 'estlbrunitcost', 'estburunitcost',
            'estsubunitcost', 'salvageestmtlunitcredit', 'salvageestlbrunitcredit', 'salvageestburunitcredit', 'salvageestsubunitcredit',
            'loanedqty', 'borrowedqty', 'reassignsnasm', 'pocostingfactor', 'plannedqtyperunit', 'pocostingdirection', 'pocostingunitval',
            'groupseq', 'sysrevid', 'sysrowid', 'showstatusicon', 'contractid', 'linktocontract', 'attributesetid', 'planningnumberofpieces',
            'relatedstage', 'salvagerevisionnum', 'partallocqueueaction', 'outstandingreqqty_c', 'qtypertool_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['jobnum', 'assemblyseq', 'mtlseq']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'jobnum, assemblyseq, mtlseq';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_JobMtl/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data JobMtl", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data JobMtl dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getBool($row, 'JobMtl_JobComplete'), $getBool($row, 'JobMtl_IssuedComplete'),
                        $getVal($row, 'JobMtl_JobNum'), $getInt($row, 'JobMtl_AssemblySeq'),
                        $getInt($row, 'JobMtl_MtlSeq'), $getVal($row, 'JobMtl_PartNum'),
                        $getVal($row, 'JobMtl_Description'), $getNum($row, 'JobMtl_QtyPer'),
                        $getNum($row, 'JobMtl_RequiredQty'), $getVal($row, 'JobMtl_IUM'),
                        $getInt($row, 'JobMtl_LeadTime'), $getInt($row, 'JobMtl_RelatedOperation'),
                        $getNum($row, 'JobMtl_MtlBurRate'), $getNum($row, 'JobMtl_EstMtlBurUnitCost'),
                        $getNum($row, 'JobMtl_EstUnitCost'), $getNum($row, 'JobMtl_IssuedQty'),
                        $getNum($row, 'JobMtl_TotalCost'), $getNum($row, 'JobMtl_MtlBurCost'),
                        $getDate($row, 'JobMtl_ReqDate'), $getVal($row, 'JobMtl_WarehouseCode'),
                        $getVal($row, 'JobMtl_SalvagePartNum'), $getVal($row, 'JobMtl_SalvageDescription'),
                        $getNum($row, 'JobMtl_SalvageQtyPer'), $getVal($row, 'JobMtl_SalvageUM'),
                        $getNum($row, 'JobMtl_SalvageMtlBurRate'), $getNum($row, 'JobMtl_SalvageUnitCredit'),
                        $getNum($row, 'JobMtl_SalvageEstMtlBurUnitCredit'), $getNum($row, 'JobMtl_SalvageQtyToDate'),
                        $getNum($row, 'JobMtl_SalvageCredit'), $getNum($row, 'JobMtl_SalvageMtlBurCredit'),
                        $getVal($row, 'JobMtl_MfgComment'), $getInt($row, 'JobMtl_VendorNum'),
                        $getVal($row, 'JobMtl_PurPoint'), $getBool($row, 'JobMtl_BuyIt'),
                        $getBool($row, 'JobMtl_Ordered'), $getVal($row, 'JobMtl_PurComment'),
                        $getBool($row, 'JobMtl_BackFlush'), $getNum($row, 'JobMtl_EstScrap'),
                        $getVal($row, 'JobMtl_EstScrapType'), $getBool($row, 'JobMtl_FixedQty'),
                        $getVal($row, 'JobMtl_FindNum'), $getVal($row, 'JobMtl_RevisionNum'),
                        $getBool($row, 'JobMtl_SndAlrtCmpl'), $getBool($row, 'JobMtl_RcvInspectionReq'),
                        $getVal($row, 'JobMtl_Plant'), $getBool($row, 'JobMtl_Direct'),
                        $getNum($row, 'JobMtl_MaterialMtlCost'), $getNum($row, 'JobMtl_MaterialLabCost'),
                        $getNum($row, 'JobMtl_MaterialSubCost'), $getNum($row, 'JobMtl_MaterialBurCost'),
                        $getNum($row, 'JobMtl_SalvageMtlCredit'), $getNum($row, 'JobMtl_SalvageLbrCredit'),
                        $getNum($row, 'JobMtl_SalvageBurCredit'), $getNum($row, 'JobMtl_SalvageSubCredit'),
                        $getVal($row, 'JobMtl_APSAddResType'), $getInt($row, 'JobMtl_CallNum'),
                        $getInt($row, 'JobMtl_CallLine'), $getVal($row, 'JobMtl_ProdCode'),
                        $getNum($row, 'JobMtl_UnitPrice'), $getNum($row, 'JobMtl_BillableUnitPrice'),
                        $getNum($row, 'JobMtl_DocBillableUnitPrice'), $getVal($row, 'JobMtl_ResReasonCode'),
                        $getVal($row, 'JobMtl_PricePerCode'), $getBool($row, 'JobMtl_Billable'),
                        $getNum($row, 'JobMtl_ShippedQty'), $getNum($row, 'JobMtl_DocUnitPrice'),
                        $getNum($row, 'JobMtl_QtyStagedToDate'), $getBool($row, 'JobMtl_AddedMtl'),
                        $getBool($row, 'JobMtl_RFQNeeded'), $getInt($row, 'JobMtl_RFQVendQuotes'),
                        $getInt($row, 'JobMtl_RFQNum'), $getInt($row, 'JobMtl_RFQLine'),
                        $getNum($row, 'JobMtl_Rpt1BillableUnitPrice'), $getNum($row, 'JobMtl_BaseRequiredQty'),
                        $getVal($row, 'JobMtl_BaseUOM'), $getNum($row, 'JobMtl_Weight'),
                        $getVal($row, 'JobMtl_WeightUOM'), $getInt($row, 'JobMtl_ReqRefDes'),
                        $getVal($row, 'JobMtl_BasePartNum'), $getVal($row, 'JobMtl_BaseRevisionNum'),
                        $getBool($row, 'JobMtl_SelectForPicking'), $getNum($row, 'JobMtl_EstMtlUnitCost'),
                        $getNum($row, 'JobMtl_EstLbrUnitCost'), $getNum($row, 'JobMtl_EstBurUnitCost'),
                        $getNum($row, 'JobMtl_EstSubUnitCost'), $getNum($row, 'JobMtl_SalvageEstMtlUnitCredit'),
                        $getNum($row, 'JobMtl_SalvageEstLbrUnitCredit'), $getNum($row, 'JobMtl_SalvageEstBurUnitCredit'),
                        $getNum($row, 'JobMtl_SalvageEstSubUnitCredit'), $getNum($row, 'JobMtl_LoanedQty'),
                        $getNum($row, 'JobMtl_BorrowedQty'), $getBool($row, 'JobMtl_ReassignSNAsm'),
                        $getNum($row, 'JobMtl_POCostingFactor'), $getNum($row, 'JobMtl_PlannedQtyPerUnit'),
                        $getInt($row, 'JobMtl_POCostingDirection'), $getNum($row, 'JobMtl_POCostingUnitVal'),
                        $getInt($row, 'JobMtl_GroupSeq'), $getInt($row, 'JobMtl_SysRevID'),
                        $getVal($row, 'JobMtl_SysRowID'), $getVal($row, 'JobMtl_ShowStatusIcon'),
                        $getVal($row, 'JobMtl_ContractID'), $getBool($row, 'JobMtl_LinkToContract'),
                        $getInt($row, 'JobMtl_AttributeSetID'), $getInt($row, 'JobMtl_PlanningNumberOfPieces'),
                        $getVal($row, 'JobMtl_RelatedStage'), $getVal($row, 'JobMtl_SalvageRevisionNum'),
                        $getVal($row, 'JobMtl_PartAllocQueueAction'), $getNum($row, 'JobMtl_OutstandingReqQty_c'),
                        $getNum($row, 'JobMtl_QtyPerTool_c'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO jobmtl ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("JobMtl Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data JobMtl ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data JobMtl Epicor selesai.',
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
    public function fetchDataJobMtl(Request $request): JsonResponse
    {
        $result = $this->syncJobMtlData();

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

