<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class PoRelController extends Controller
{
    /**
     * Mengambil data PoRel dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncPoRelData(): array
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
            'openrelease', 'voidrelease', 'ponum', 'poline', 'porelnum', 'duedate', 'xrelqty', 'purchasingfactor',
            'relqty', 'jobnum', 'assemblyseq', 'jobseqtype', 'jobseq', 'warehousecode', 'receivedqty', 'expoverride',
            'reqnum', 'reqline', 'plant', 'promisedt', 'confirmed', 'confirmvia', 'reqchgdate', 'reqchgqty', 'lockrel',
            'reftype', 'refcode', 'refcodedesc', 'ordernum', 'orderline', 'orderrelnum', 'shippedqty', 'trantype', 
            'shippeddate', 'containerid', 'purchasingfactordirection', 'previousduedate', 'baseqty', 'baseuom', 'btoordernum',
            'btoorderline', 'btoorderrelnum', 'dropship', 'shiptonum', 'soldtonum', 'smircvdqty', 'shpconnum', 'shiptocustnum',
            'mangcustnum', 'useots', 'otsname', 'otsaddress1', 'otsaddress2', 'otsaddress3', 'otscity', 'otsstate',
            'otszip', 'otsresaleid', 'otscontact', 'otsfaxnum', 'otsphonenum', 'otscountrynum', 'compliancemsg', 'porelopen',
            'sysrevid', 'sysrowid', 'smiremqty', 'lockqty', 'lockdate', 'duedatechanged', 'status', 'arrivedqty',
            'invoicedqty', 'needbydate', 'lockneedbydate', 'inspectionqty', 'failedqty', 'passedqty', 'deliverto', 'taxable',
            'taxexempt', 'notaxrecalc', 'reqchgpromisedate', 'attributesetid', 'numberofpieces', 'numberofpiecesuom', 
            'planningnumberofpieces', 'firmrelease', 'itlegalnumber_c', 'changedate'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['ponum', 'poline', 'porelnum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'ponum, poline, porelnum';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_PoRel/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data PoRel", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data PoRel dari API',
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
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getBool($row, 'PORel_OpenRelease'), $getBool($row, 'PORel_VoidRelease'),
                        $getInt($row, 'PORel_PONum'), $getInt($row, 'PORel_POLine'),
                        $getInt($row, 'PORel_PORelNum'), $getDate($row, 'PORel_DueDate'),
                        $getNum($row, 'PORel_XRelQty'), $getNum($row, 'PORel_PurchasingFactor'),
                        $getNum($row, 'PORel_RelQty'), $getVal($row, 'PORel_JobNum'),
                        $getInt($row, 'PORel_AssemblySeq'), $getVal($row, 'PORel_JobSeqType'),
                        $getInt($row, 'PORel_JobSeq'), $getVal($row, 'PORel_WarehouseCode'),
                        $getNum($row, 'PORel_ReceivedQty'), $getBool($row, 'PORel_ExpOverride'),
                        $getInt($row, 'PORel_ReqNum'), $getInt($row, 'PORel_ReqLine'),
                        $getVal($row, 'PORel_Plant'), $getDate($row, 'PORel_PromiseDt'),
                        $getBool($row, 'PORel_Confirmed'), $getVal($row, 'PORel_ConfirmVia'),
                        $getDate($row, 'PORel_ReqChgDate'), $getNum($row, 'PORel_ReqChgQty'),
                        $getVal($row, 'PORel_LockRel'), $getVal($row, 'PORel_RefType'),
                        $getVal($row, 'PORel_RefCode'), $getVal($row, 'PORel_RefCodeDesc'),
                        $getInt($row, 'PORel_OrderNum'), $getInt($row, 'PORel_OrderLine'),
                        $getInt($row, 'PORel_OrderRelNum'), $getNum($row, 'PORel_ShippedQty'),
                        $getVal($row, 'PORel_TranType'), $getDate($row, 'PORel_ShippedDate'),
                        $getInt($row, 'PORel_ContainerID'), $getVal($row, 'PORel_PurchasingFactorDirection'),
                        $getDate($row, 'PORel_PreviousDueDate'), $getNum($row, 'PORel_BaseQty'),
                        $getVal($row, 'PORel_BaseUOM'), $getInt($row, 'PORel_BTOOrderNum'),
                        $getInt($row, 'PORel_BTOOrderLine'), $getInt($row, 'PORel_BTOOrderRelNum'),
                        $getBool($row, 'PORel_DropShip'), $getVal($row, 'PORel_ShipToNum'),
                        $getInt($row, 'PORel_SoldToNum'), $getNum($row, 'PORel_SMIRcvdQty'),
                        $getInt($row, 'PORel_ShpConNum'), $getInt($row, 'PORel_ShipToCustNum'),
                        $getInt($row, 'PORel_MangCustNum'), $getBool($row, 'PORel_UseOTS'),
                        $getVal($row, 'PORel_OTSName'), $getVal($row, 'PORel_OTSAddress1'),
                        $getVal($row, 'PORel_OTSAddress2'), $getVal($row, 'PORel_OTSAddress3'),
                        $getVal($row, 'PORel_OTSCity'), $getVal($row, 'PORel_OTSState'),
                        $getVal($row, 'PORel_OTSZIP'), $getVal($row, 'PORel_OTSResaleID'),
                        $getVal($row, 'PORel_OTSContact'), $getVal($row, 'PORel_OTSFaxNum'),
                        $getVal($row, 'PORel_OTSPhoneNum'), $getInt($row, 'PORel_OTSCountryNum'),
                        $getVal($row, 'PORel_ComplianceMsg'), $getBool($row, 'PORel_PORelOpen'),
                        $getInt($row, 'PORel_SysRevID'), $getVal($row, 'PORel_SysRowID'),
                        $getNum($row, 'PORel_SMIRemQty'), $getBool($row, 'PORel_LockQty'),
                        $getBool($row, 'PORel_LockDate'), $getBool($row, 'PORel_DueDateChanged'),
                        $getVal($row, 'PORel_Status'), $getNum($row, 'PORel_ArrivedQty'),
                        $getNum($row, 'PORel_InvoicedQty'), $getDate($row, 'PORel_NeedByDate'),
                        $getBool($row, 'PORel_LockNeedByDate'), $getNum($row, 'PORel_InspectionQty'),
                        $getNum($row, 'PORel_FailedQty'), $getNum($row, 'PORel_PassedQty'),
                        $getVal($row, 'PORel_DeliverTo'), $getBool($row, 'PORel_Taxable'),
                        $getVal($row, 'PORel_TaxExempt'), $getBool($row, 'PORel_NoTaxRecalc'),
                        $getDate($row, 'PORel_ReqChgPromiseDate'), $getInt($row, 'PORel_AttributeSetID'),
                        $getInt($row, 'PORel_NumberOfPieces'), $getVal($row, 'PORel_NumberOfPiecesUOM'),
                        $getInt($row, 'PORel_PlanningNumberOfPieces'), $getBool($row, 'PORel_FirmRelease'),
                        $getVal($row, 'PORel_ITLegalNumber_c'), $getTimestamp($row, 'Calculated_changedate'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO porel ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("PoRel Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data PoRel ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data PoRel Epicor selesai.',
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
    public function fetchDataPoRel(Request $request): JsonResponse
    {
        $result = $this->syncPoRelData();

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

