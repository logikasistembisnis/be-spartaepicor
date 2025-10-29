<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\Log;

class PartTranController extends Controller
{
    /**
     * Public method for syncing PartTran data from Epicor API.
     * For artisan command.
     * @return array Result or error
     */
    public function syncPartTranData(): array
    {
        // Konstanta: UKURAN BATCH INTERNAL PHP untuk INSERT ke database.
        $INTERNAL_BATCH_SIZE = 500; 

        // Mengatur batas memori dan waktu
        ini_set('memory_limit', '512M');
        set_time_limit(1200);

        // Ambil last trannum dari database.
        $lastTranNum = DB::selectOne('SELECT COALESCE(MAX(trannum), 0) AS lasttrannum FROM parttran')->lasttrannum;
        $batchCount = 0; 
        $totalInserted = 0;
        $maxTranNumProcessed = $lastTranNum;

        $columnNames = [
            'trannum', 'sysdate', 'systime', 'partnum', 'warehousecode', 'binnum',
            'trantype', 'trandate', 'tranqty', 'um', 'mtlunitcost', 'lbrunitcost',
            'burunitcost', 'subunitcost', 'mtlburunitcost', 'extcost', 'costmethod',
            'jobnum', 'assemblyseq', 'jobseqtype', 'jobseq', 'packnum', 'packslip',
            'packline', 'ponum', 'poline', 'porelnum', 'warehouse2', 'binnum2',
            'ordernum', 'orderline', 'orderrelnum', 'entryperson', 'tranreference',
            'partdescription', 'revisionnum', 'vendornum', 'purpoint', 'poreceiptqty',
            'invoicenum', 'invoiceline', 'invadjsrc', 'invadjreason', 'lotnum',
            'custnum', 'rmanum', 'rmaline', 'rmareceipt', 'rmadisp', 'legalnumber',
            'mscshp_packnum_c', 'mscshp_packline_c', 'po_poline_c', 'po_ponum_c',
            'sugpo_jobnum_c', 'sugpo_asm_c', 'sugpo_mtl_c', 'laborhedid_c',
            'labordtlid_c', 'imshift_c', 'it_jobnum_c', 'tool_resourceid_c',
            'rcvdtlpackslip_c', 'rcvdtlpackline_c', 'po_porelnum_c',
            'toolsusedenddate_c', 'sjplantpacknum_c', 'sjplantpackline_c',
            'sugpo_vendornum_c', 'bkbsubcontid_c', 'bkbsubcontline_c', 'qtybtg_c',
            'partnumusedtool_c', 'plantused_c', 'panjang_c', 'batang_c',
            'panjang1_c', 'batang1_c', 'panjang2_c', 'batang2_c', 'panjang3_c',
            'batang3_c', 'panjang4_c', 'batang4_c', 'panjang5_c', 'batang5_c'
        ];
        
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';

        // LOOP
        do {
            // Ambil data dari API Epicor
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'), 
                'License' => env('EPICOR_LICENSE'), 
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_PARTTRAN_API_URL'), [
                'StartTranNum' => $lastTranNum,
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                $errorMsg = "Gagal mengambil data dari API Epicor. TranNum: {$lastTranNum}";
                Log::error($errorMsg, ['status' => $status, 'body' => $errorBody]);
                
                return [
                    'error' => $errorMsg, 
                    'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            if (empty($data)) break;

            // LOOP INTERNAL (PHP BATCHING)
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);

            foreach ($dataChunks as $chunk) {
                $dataToInsert = []; 
                $bindValues = []; 
                
                foreach ($chunk as $row) {
                    // Mapping data dari Epicor ke array $rowData
                    $rowData = [
                        (int)$row['PartTran_TranNum'] ?? 0,
                        isset($row['PartTran_SysDate']) ? substr($row['PartTran_SysDate'], 0, 10) : null,
                        (int)$row['PartTran_SysTime'] ?? 0,
                        $row['PartTran_PartNum'] ?? null,
                        $row['PartTran_WareHouseCode'] ?? null,
                        $row['PartTran_BinNum'] ?? null,
                        $row['PartTran_TranType'] ?? null,
                        isset($row['PartTran_TranDate']) ? substr($row['PartTran_TranDate'], 0, 10) : null,
                        (float)$row['PartTran_TranQty'] ?? 0,
                        $row['PartTran_UM'] ?? null,
                        (float)$row['PartTran_MtlUnitCost'] ?? 0,
                        (float)$row['PartTran_LbrUnitCost'] ?? 0,
                        (float)$row['PartTran_BurUnitCost'] ?? 0,
                        (float)$row['PartTran_SubUnitCost'] ?? 0,
                        (float)$row['PartTran_MtlBurUnitCost'] ?? 0,
                        (float)$row['PartTran_ExtCost'] ?? 0,
                        $row['PartTran_CostMethod'] ?? null,
                        $row['PartTran_JobNum'] ?? null,
                        (int)$row['PartTran_AssemblySeq'] ?? 0,
                        $row['PartTran_JobSeqType'] ?? null,
                        (int)$row['PartTran_JobSeq'] ?? 0,
                        (int)$row['PartTran_PackNum'] ?? 0,
                        $row['PartTran_PackSlip'] ?? 0,
                        (int)$row['PartTran_PackLine'] ?? 0,
                        (int)$row['PartTran_PONum'] ?? 0,
                        (int)$row['PartTran_POLine'] ?? 0,
                        (int)$row['PartTran_PORelNum'] ?? 0,
                        $row['PartTran_WareHouse2'] ?? null,
                        $row['PartTran_BinNum2'] ?? null,
                        (int)$row['PartTran_OrderNum'] ?? 0,
                        (int)$row['PartTran_OrderLine'] ?? 0,
                        (int)$row['PartTran_OrderRelNum'] ?? 0,
                        $row['PartTran_EntryPerson'] ?? null,
                        $row['PartTran_TranReference'] ?? null,
                        $row['PartTran_PartDescription'] ?? null,
                        $row['PartTran_RevisionNum'] ?? null,
                        (int)$row['PartTran_VendorNum'] ?? 0,
                        $row['PartTran_PurPoint'] ?? null,
                        (float)$row['PartTran_POReceiptQty'] ?? 0,
                        $row['PartTran_InvoiceNum'] ?? null,
                        (int)$row['PartTran_InvoiceLine'] ?? 0,
                        $row['PartTran_InvAdjSrc'] ?? null,
                        $row['PartTran_InvAdjReason'] ?? null,
                        $row['PartTran_LotNum'] ?? null,
                        (int)$row['PartTran_CustNum'] ?? 0,
                        (int)$row['PartTran_RMANum'] ?? 0,
                        (int)$row['PartTran_RMALine'] ?? 0,
                        (int)$row['PartTran_RMAReceipt'] ?? 0,
                        (int)$row['PartTran_RMADisp'] ?? 0,
                        $row['PartTran_LegalNumber'] ?? null,
                        (int)$row['PartTran_MscShp_PackNum_c'] ?? 0,
                        (int)$row['PartTran_MscShp_PackLine_c'] ?? 0,
                        (int)$row['PartTran_PO_POLine_c'] ?? 0,
                        (int)$row['PartTran_PO_PONum_c'] ?? 0,
                        $row['PartTran_SugPO_JobNum_c'] ?? null,
                        (int)$row['PartTran_SugPO_Asm_c'] ?? 0,
                        (int)$row['PartTran_SugPO_Mtl_c'] ?? 0,
                        (int)$row['PartTran_LaborHedID_c'] ?? 0,
                        (int)$row['PartTran_LaborDtlID_c'] ?? 0,
                        $row['PartTran_IMShift_c'] ?? null,
                        $row['PartTran_IT_JobNum_c'] ?? null,
                        $row['PartTran_Tool_ResourceID_c'] ?? null,
                        $row['PartTran_RcvDtlPackSlip_c'] ?? null,
                        (int)$row['PartTran_RcvDtlPackLine_c'] ?? 0,
                        (int)$row['PartTran_PO_PORelNum_c'] ?? 0,
                        isset($row['PartTran_ToolsUsedEndDate_c']) ? substr($row['PartTran_ToolsUsedEndDate_c'], 0, 10) : null,
                        $row['PartTran_SJPlantPacknum_c'] ?? null,
                        (int)$row['PartTran_SJPlantPackLine_c'] ?? 0,
                        (int)$row['PartTran_SugPO_VendorNum_c'] ?? 0,
                        $row['PartTran_BKBSubcontID_c'] ?? null,
                        (int)$row['PartTran_BKBSubcontLine_c'] ?? 0,
                        (float)$row['PartTran_QtyBtg_c'] ?? 0,
                        $row['PartTran_PartNumUsedTool_c'] ?? null,
                        $row['PartTran_PlantUsed_c'] ?? null,
                        (float)$row['PartTran_Panjang_c'] ?? 0,
                        (float)$row['PartTran_Batang_c'] ?? 0,
                        (float)$row['PartTran_Panjang1_c'] ?? 0,
                        (float)$row['PartTran_Batang1_c'] ?? 0,
                        (float)$row['PartTran_Panjang2_c'] ?? 0,
                        (float)$row['PartTran_Batang2_c'] ?? 0,
                        (float)$row['PartTran_Panjang3_c'] ?? 0,
                        (float)$row['PartTran_Batang3_c'] ?? 0,
                        (float)$row['PartTran_Panjang4_c'] ?? 0,
                        (float)$row['PartTran_Batang4_c'] ?? 0,
                        (float)$row['PartTran_Panjang5_c'] ?? 0,
                        (float)$row['PartTran_Batang5_c'] ?? 0,
                    ];

                    $dataToInsert[] = $rowData;
                    array_push($bindValues, ...$rowData);
                    
                    if (($row['PartTran_TranNum'] ?? 0) > $maxTranNumProcessed) {
                        $maxTranNumProcessed = $row['PartTran_TranNum'];
                    }
                }

                if (!empty($dataToInsert)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($dataToInsert), $placeholderRow));

                        $sql = "
                            INSERT INTO parttran ({$columnsSql})
                            VALUES {$placeholderRows}
                            ON CONFLICT (trannum) DO NOTHING
                        ";

                        DB::insert($sql, $bindValues);
                        $insertCount = count($dataToInsert); 
                        
                        DB::commit();
                        $batchCount++;
                        $totalInserted += $insertCount;
                    } catch (\Exception $e) {
                        DB::rollBack();
                        $errorMsg = "Epicor Batch Insert Gagal.";
                        Log::error($errorMsg, ['error' => $e->getMessage()]);
                        return ['error' => 'Gagal memasukkan data ke database.', 'details' => $e->getMessage()];
                    }
                }

            } 

            $lastTranNum = $maxTranNumProcessed;
            
        } while (count($data) === 5000);

        return [
            'total_inserted' => $totalInserted,
            'last_trannum_processed' => $maxTranNumProcessed,
            'total_batches_processed' => $batchCount
        ];
    }
    
    /**
     * HTTP Endpoint for fetching data from Epicor API.
     */
    public function fetchDataPartTran(): JsonResponse
    {
        $result = $this->syncPartTranData();

        if (isset($result['error'])) {
            return response()->json([
                'message' => 'Gagal sinkronisasi data Epicor.',
                'error' => $result['error'],
                'details' => $result['details'] ?? null
            ], 500);
        }

        return response()->json([
            'message' => 'Sinkronisasi data Epicor selesai.',
            'total_inserted' => $result['total_inserted'],
            'last_trannum_processed' => $result['last_trannum_processed'],
            'total_batches_processed' => $result['total_batches_processed']
        ]);
    }
}
