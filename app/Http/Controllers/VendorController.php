<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class VendorController extends Controller
{
    /**
     * Mengambil data Vendor dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncVendorData(): array
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
            'vendorid', 'name', 'vendornum', 'address1', 'address2', 'address3', 'city', 'state', 'zip', 
            'country', 'taxpayerid', 'purpoint', 'termscode', 'groupcode', 'printlabels', 'faxnum', 'phonenum', 
            'comment', 'payhold', 'rcvinspectionreq', 'currencycode', 'taxregioncode', 'countrynum', 'approved', 
            'icvend', 'emailaddress', 'consolidatedpurchasing', 'localpurchasing', 'cpay', 'individualpackids', 
            'certoforigin', 'commercialinvoice', 'shipexprtdeclartn', 'letterofinstr', 'nonstdpkg', 'deliveryconf', 
            'pmuid', 'hasbank', 'pmtacctref', 'taxregreason', 'orgregcode', 'sysrevid', 'sysrowid', 'paramcode', 
            'maxlatedaysporel', 'shipviacode', 'nonus', 'taxvalidationstatus', 'taxvalidationdate'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['vendorid']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'vendorid';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_Vendor/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data Vendor", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data Vendor dari API',
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
                $getTimestamp = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'Vendor_VendorID'), $getVal($row, 'Vendor_Name'),
                        $getInt($row, 'Vendor_VendorNum'), $getVal($row, 'Vendor_Address1'),
                        $getVal($row, 'Vendor_Address2'), $getVal($row, 'Vendor_Address3'),
                        $getVal($row, 'Vendor_City'), $getVal($row, 'Vendor_State'),
                        $getVal($row, 'Vendor_ZIP'), $getVal($row, 'Vendor_Country'),
                        $getVal($row, 'Vendor_TaxPayerID'), $getVal($row, 'Vendor_PurPoint'),
                        $getVal($row, 'Vendor_TermsCode'), $getVal($row, 'Vendor_GroupCode'),
                        $getBool($row, 'Vendor_PrintLabels'), $getVal($row, 'Vendor_FaxNum'),
                        $getVal($row, 'Vendor_PhoneNum'), $getVal($row, 'Vendor_Comment'),
                        $getBool($row, 'Vendor_PayHold'), $getBool($row, 'Vendor_RcvInspectionReq'),
                        $getVal($row, 'Vendor_CurrencyCode'), $getVal($row, 'Vendor_TaxRegionCode'),
                        $getInt($row, 'Vendor_CountryNum'), $getBool($row, 'Vendor_Approved'),
                        $getBool($row, 'Vendor_ICVend'), $getVal($row, 'Vendor_EMailAddress'),
                        $getBool($row, 'Vendor_ConsolidatedPurchasing'), $getBool($row, 'Vendor_LocalPurchasing'),
                        $getBool($row, 'Vendor_CPay'), $getBool($row, 'Vendor_IndividualPackIDs'),
                        $getBool($row, 'Vendor_CertOfOrigin'), $getBool($row, 'Vendor_CommercialInvoice'),
                        $getBool($row, 'Vendor_ShipExprtDeclartn'), $getBool($row, 'Vendor_LetterOfInstr'),
                        $getBool($row, 'Vendor_NonStdPkg'), $getInt($row, 'Vendor_DeliveryConf'),
                        $getInt($row, 'Vendor_PMUID'), $getBool($row, 'Vendor_HasBank'),
                        $getVal($row, 'Vendor_PmtAcctRef'), $getVal($row, 'Vendor_TaxRegReason'),
                        $getVal($row, 'Vendor_OrgRegCode'), $getInt($row, 'Vendor_SysRevID'),
                        $getVal($row, 'Vendor_SysRowID'), $getVal($row, 'Vendor_ParamCode'),
                        $getInt($row, 'Vendor_MaxLateDaysPORel'), $getVal($row, 'Vendor_ShipViaCode'),
                        $getBool($row, 'Vendor_NonUS'), $getInt($row, 'Vendor_TaxValidationStatus'),
                        $getTimestamp($row, 'Vendor_TaxValidationDate')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO vendor ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("Vendor Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data Vendor ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data Vendor Epicor selesai.',
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
    public function fetchDataVendor(Request $request): JsonResponse
    {
        $result = $this->syncVendorData();

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

