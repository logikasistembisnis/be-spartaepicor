<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class RcvDtlController extends Controller
{
    public function syncRcvDtlData(?string $period = null, ?string $startDate = null):array
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
        $maxPackSlipProcessed = 0;
        $maxPackLineProcessed = 0;

        $columnNames = [
            'vendornum', 'purpoint', 'packslip', 'packline', 'invoiced', 'invoicenum', 'invoiceline', 'partnum', 'warehousecode', 'binnum', 'ourqty', 
            'ium', 'ourunitcost', 'jobnum', 'assemblyseq', 'jobseqtype', 'jobseq', 'ponum', 'poline', 'porelnum', 'tranreference', 'partdescription', 
            'revisionnum', 'vendorqty', 'vendorunitcost', 'receivedto', 'receivedcomplete', 'issuedcomplete', 'pum', 'venpartnum', 'costpercode', 
            'lotnum', 'numlabels', 'inspectionreq', 'inspectionpending', 'inspectorid', 'inspectedby', 'inspecteddate', 'inspectedtime', 'passedqty', 
            'failedqty', 'receiptdate', 'reasoncode', 'totcostvariance', 'nonconformnce', 'mtlburrate', 'ourmtlburunitcost', 'reftype', 'docunitcost', 
            'containerid', 'volume', 'potransvalue', 'exttransvalue', 'received', 'potype', 'volumeuom', 'arriveddate', 'docvendorunitcost', 
            'rpt1vendorunitcost', 'rpt2vendorunitcost', 'rpt3vendorunitcost', 'legalnumber', 'shiprcv', 'importnum', 'importedfrom', 'importedfromdesc', 
            'grossweight', 'grossweightuom', 'convoverride', 'smitransnum', 'changedby', 'changedate', 'delivered', 'deliveredcomments', 'inourcost', 
            'docinunitcost', 'docinvendorunitcost', 'supplieruninvcreceiptqty', 'ouruninvcreceiptqty', 'taxregioncode', 'taxcatid', 'taxable', 'taxexempt', 
            'notaxrecalc', 'assetnum', 'qtydoc_c', 'qcqtypass_c', 'qcqtyfail_c', 'qcdate_c', 'qcinspectedby_c', 'qcfailremark_c', 'rcvdtlpackslip_c', 
            'rcvdtlpackline_c', 'qtyreturn_c', 'panjang1_c', 'batang1_c', 'panjang2_c', 'batang2_c', 'panjang3_c', 'batang3_c', 'panjang4_c', 'batang4_c', 
            'panjang5_c', 'batang5_c', 'panjang_c', 'batang_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['packslip', 'packline']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'packslip, packline';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_RcvDtl/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data LaborDtl", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data LaborDtl dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $chunk = collect($chunk)
                ->sortBy('UpdatedOn')
                ->unique(fn($r) => $r['RcvDtl_PackSlip'] . '-' . $r['RcvDtl_PackLine'])
                ->values()
                ->toArray();
                
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getFloat = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $PackSlip = $getInt($row, 'RcvDtl_PackSlip');
                    $PackLine = $getInt($row, 'RcvDtl_PackLine');

                    $rowData = [
                        $getInt($row, 'RcvDtl_VendorNum'),
                        $getVal($row, 'RcvDtl_PurPoint',),
                        $getVal($row, 'RcvDtl_PackSlip'),
                        $getInt($row, 'RcvDtl_PackLine'),
                        $getBool($row, 'RcvDtl_Invoiced'),
                        $getVal($row, 'RcvDtl_InvoiceNum',),
                        $getInt($row, 'RcvDtl_InvoiceLine'),
                        $getVal($row, 'RcvDtl_PartNum'),
                        $getVal($row, 'RcvDtl_WareHouseCode'),
                        $getVal($row, 'RcvDtl_BinNum'),
                        $getFloat($row, 'RcvDtl_OurQty'),
                        $getVal($row, 'RcvDtl_IUM'),
                        $getFloat($row, 'RcvDtl_OurUnitCost'),
                        $getVal($row, 'RcvDtl_JobNum',),
                        $getInt($row, 'RcvDtl_AssemblySeq'),
                        $getVal($row, 'RcvDtl_JobSeqType',),
                        $getInt($row, 'RcvDtl_JobSeq'),
                        $getInt($row, 'RcvDtl_PONum'),
                        $getInt($row, 'RcvDtl_POLine'),
                        $getInt($row, 'RcvDtl_PORelNum'),
                        $getVal($row, 'RcvDtl_TranReference',),
                        $getVal($row, 'RcvDtl_PartDescription'),
                        $getVal($row, 'RcvDtl_RevisionNum',),
                        $getFloat($row, 'RcvDtl_VendorQty'),
                        $getFloat($row, 'RcvDtl_VendorUnitCost'),
                        $getVal($row, 'RcvDtl_ReceivedTo'),
                        $getBool($row, 'RcvDtl_ReceivedComplete'),
                        $getBool($row, 'RcvDtl_IssuedComplete'),
                        $getVal($row, 'RcvDtl_PUM'),
                        $getVal($row, 'RcvDtl_VenPartNum',),
                        $getVal($row, 'RcvDtl_CostPerCode'),
                        $getVal($row, 'RcvDtl_LotNum'),
                        $getInt($row, 'RcvDtl_NumLabels'),
                        $getBool($row, 'RcvDtl_InspectionReq'),
                        $getBool($row, 'RcvDtl_InspectionPending'),
                        $getVal($row, 'RcvDtl_InspectorID',),
                        $getVal($row, 'RcvDtl_InspectedBy',),
                        $getDate($row, 'RcvDtl_InspectedDate'),
                        $getInt($row, 'RcvDtl_InspectedTime'),
                        $getFloat($row, 'RcvDtl_PassedQty'),
                        $getFloat($row, 'RcvDtl_FailedQty'),
                        $getDate($row, 'RcvDtl_ReceiptDate'),
                        $getVal($row, 'RcvDtl_ReasonCode',),
                        $getFloat($row, 'RcvDtl_TotCostVariance'),
                        $getBool($row, 'RcvDtl_NonConformnce'),
                        $getFloat($row, 'RcvDtl_MtlBurRate'),
                        $getFloat($row, 'RcvDtl_OurMtlBurUnitCost'),
                        $getVal($row, 'RcvDtl_RefType',),
                        $getFloat($row, 'RcvDtl_DocUnitCost'),
                        $getInt($row, 'RcvDtl_ContainerID'),
                        $getFloat($row, 'RcvDtl_Volume'),
                        $getFloat($row, 'RcvDtl_POTransValue'),
                        $getFloat($row, 'RcvDtl_ExtTransValue'),
                        $getBool($row, 'RcvDtl_Received'),
                        $getVal($row, 'RcvDtl_POType'),
                        $getVal($row, 'RcvDtl_VolumeUOM'),
                        $getDate($row, 'RcvDtl_ArrivedDate'),
                        $getFloat($row, 'RcvDtl_DocVendorUnitCost'),
                        $getFloat($row, 'RcvDtl_Rpt1VendorUnitCost'),
                        $getFloat($row, 'RcvDtl_Rpt2VendorUnitCost'),
                        $getFloat($row, 'RcvDtl_Rpt3VendorUnitCost'),
                        $getVal($row, 'RcvDtl_LegalNumber'),
                        $getVal($row, 'RcvDtl_ShipRcv',),
                        $getVal($row, 'RcvDtl_ImportNum',),
                        $getInt($row, 'RcvDtl_ImportedFrom'),
                        $getVal($row, 'RcvDtl_ImportedFromDesc'),
                        $getFloat($row, 'RcvDtl_GrossWeight'),
                        $getVal($row, 'RcvDtl_GrossWeightUOM'),
                        $getBool($row, 'RcvDtl_ConvOverride'),
                        $getInt($row, 'RcvDtl_SMITransNum'),
                        $getVal($row, 'RcvDtl_ChangedBy'),
                        $getDate($row, 'RcvDtl_ChangeDate'),
                        $getBool($row, 'RcvDtl_Delivered'),
                        $getVal($row, 'RcvDtl_DeliveredComments',),
                        $getFloat($row, 'RcvDtl_InOurCost'),
                        $getFloat($row, 'RcvDtl_DocInUnitCost'),
                        $getFloat($row, 'RcvDtl_DocInVendorUnitCost'),
                        $getFloat($row, 'RcvDtl_SupplierUnInvcReceiptQty'),
                        $getFloat($row, 'RcvDtl_OurUnInvcReceiptQty'),
                        $getVal($row, 'RcvDtl_TaxRegionCode',),
                        $getVal($row, 'RcvDtl_TaxCatID',),
                        $getBool($row, 'RcvDtl_Taxable'),
                        $getVal($row, 'RcvDtl_TaxExempt',),
                        $getBool($row, 'RcvDtl_NoTaxRecalc'),
                        $getVal($row, 'RcvDtl_AssetNum',),
                        $getFloat($row, 'RcvDtl_qtydoc_c'),
                        $getFloat($row, 'RcvDtl_qcqtypass_c'),
                        $getFloat($row, 'RcvDtl_qcqtyfail_c'),
                        $getDate($row, 'RcvDtl_qcdate_c'),
                        $getVal($row, 'RcvDtl_qcinspectedby_c',),
                        $getVal($row, 'RcvDtl_qcfailremark_c',),
                        $getVal($row, 'RcvDtl_RcvDtlPackSlip_c',),
                        $getInt($row, 'RcvDtl_RcvDtlPackLine_c'),
                        $getFloat($row, 'RcvDtl_qtyreturn_c'),
                        $getFloat($row, 'RcvDtl_Panjang1_c'),
                        $getFloat($row, 'RcvDtl_Batang1_c'),
                        $getFloat($row, 'RcvDtl_Panjang2_c'),
                        $getFloat($row, 'RcvDtl_Batang2_c'),
                        $getFloat($row, 'RcvDtl_Panjang3_c'),
                        $getFloat($row, 'RcvDtl_Batang3_c'),
                        $getFloat($row, 'RcvDtl_Panjang4_c'),
                        $getFloat($row, 'RcvDtl_Batang4_c'),
                        $getFloat($row, 'RcvDtl_Panjang5_c'),
                        $getFloat($row, 'RcvDtl_Batang5_c'),
                        $getVal($row, 'RcvDtl_panjang_c',),
                        $getVal($row, 'RcvDtl_batang_c',)
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                    $maxPackSlipProcessed = max($maxPackSlipProcessed, $PackSlip);
                    $maxPackLineProcessed = max($maxPackLineProcessed, $PackLine);
                }

           if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO rcvdtl ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("RcvDtl Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data RcvDtl ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data RcvDtl Epicor selesai.',
            'filter_start_date' => $startDate,
            'filter_period' => $period,
            'total_processed_api_rows' => $totalProcessed,
            'total_db_batches_processed' => $batchCount,
            'last_packslip_in_data' => $maxPackSlipProcessed,
            'last_packline_in_data' => $maxPackLineProcessed,
        ];
    }
    public function fetchDataRcvDtl(Request $request): JsonResponse
    {
        // Ambil parameter dari query string (bisa null)
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        // Panggil fungsi inti. 
        $result = $this->syncLaborDtlData($period, $startDate);

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