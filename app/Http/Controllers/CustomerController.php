<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class CustomerController extends Controller
{
    /**
     * Mengambil data Customer dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncCustomerData(): array
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
            'custid', 'custnum', 'name', 'address1', 'address2', 'address3', 'city', 'state', 'zip', 'country', 
            'resaleid', 'shiptonum', 'termscode', 'shipviacode', 'printstatements', 'printlabels', 'fincharges', 
            'credithold', 'discountpercent', 'primpcon', 'primbcon', 'primscon', 'comment', 'estdate', 'faxnum', 
            'phonenum',  'billday', 'creditholdsource', 'currencycode', 'countrynum', 'btname', 'btaddress1', 
            'btaddress2', 'btaddress3', 'btcity', 'btstate', 'btzip', 'btcountrynum', 'btcountry', 'btphonenum', 
            'btfaxnum', 'btformatstr', 'parentcustnum', 'taxregioncode', 'contbillday', 'emailaddress', 
            'customertype', 'consolidateso', 'billfrequency', 'taxauthoritycode', 'externaldeliverynote', 
            'checkduplicatepo', 'creditlimit', 'custpilimit', 'refnotes', 'applychrg', 'chrgamount', 'notifyflag', 
            'notifyemail', 'changedby', 'changedate', 'changetime', 'certoforigin', 'commercialinvoice', 
            'shipexprtdeclartn', 'nonstdpkg', 'deliveryconf', 'taxroundrule', 'taxmethod', 'invperpackline', 
            'orgregcode', 'periodicbilling', 'duedatecriteria', 'pbterms', 'sysrevid', 'sysrowid', 'districtname', 
            'streetname', 'buildingnumber', 'floor', 'room', 'postbox', 'btdistrictname', 'btstreetname', 
            'btbuildingnumber', 'btfloor', 'btroom', 'btpostbox', 'creditholdreason', 'creditholdnote'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['custid']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'custid';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_Customer/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data Customer", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data Customer dari API',
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
                        $getVal($row, 'Customer_CustID'), $getInt($row, 'Customer_CustNum'),
                        $getVal($row, 'Customer_Name'), $getVal($row, 'Customer_Address1'),
                        $getVal($row, 'Customer_Address2'), $getVal($row, 'Customer_Address3'),
                        $getVal($row, 'Customer_City'), $getVal($row, 'Customer_State'),
                        $getVal($row, 'Customer_Zip'), $getVal($row, 'Customer_Country'),
                        $getVal($row, 'Customer_ResaleID'), $getVal($row, 'Customer_ShipToNum'),
                        $getVal($row, 'Customer_TermsCode'), $getVal($row, 'Customer_ShipViaCode'),
                        $getBool($row, 'Customer_PrintStatements'), $getBool($row, 'Customer_PrintLabels'),
                        $getBool($row, 'Customer_FinCharges'), $getBool($row, 'Customer_CreditHold'),
                        $getNum($row, 'Customer_DiscountPercent'), $getInt($row, 'Customer_PrimPCon'),
                        $getInt($row, 'Customer_PrimBCon'), $getInt($row, 'Customer_PrimSCon'),
                        $getVal($row, 'Customer_Comment'), $getDate($row, 'Customer_EstDate'),
                        $getVal($row, 'Customer_FaxNum'), $getVal($row, 'Customer_PhoneNum'),
                        $getInt($row, 'Customer_BillDay'), $getVal($row, 'Customer_CreditHoldSource'),
                        $getVal($row, 'Customer_CurrencyCode'), $getInt($row, 'Customer_CountryNum'),
                        $getVal($row, 'Customer_BTName'), $getVal($row, 'Customer_BTAddress1'),
                        $getVal($row, 'Customer_BTAddress2'), $getVal($row, 'Customer_BTAddress3'),
                        $getVal($row, 'Customer_BTCity'), $getVal($row, 'Customer_BTState'),
                        $getVal($row, 'Customer_BTZip'), $getInt($row, 'Customer_BTCountryNum'),
                        $getVal($row, 'Customer_BTCountry'), $getVal($row, 'Customer_BTPhoneNum'),
                        $getVal($row, 'Customer_BTFaxNum'), $getVal($row, 'Customer_BTFormatStr'),
                        $getInt($row, 'Customer_ParentCustNum'), $getVal($row, 'Customer_TaxRegionCode'),
                        $getInt($row, 'Customer_ContBillDay'), $getVal($row, 'Customer_EMailAddress'),
                        $getVal($row, 'Customer_CustomerType'), $getBool($row, 'Customer_ConsolidateSO'),
                        $getVal($row, 'Customer_BillFrequency'), $getVal($row, 'Customer_TaxAuthorityCode'),
                        $getBool($row, 'Customer_ExternalDeliveryNote'), $getBool($row, 'Customer_CheckDuplicatePO'),
                        $getNum($row, 'Customer_CreditLimit'), $getNum($row, 'Customer_CustPILimit'),
                        $getVal($row, 'Customer_RefNotes'), $getBool($row, 'Customer_ApplyChrg'),
                        $getNum($row, 'Customer_ChrgAmount'), $getBool($row, 'Customer_NotifyFlag'),
                        $getVal($row, 'Customer_NotifyEMail'), $getVal($row, 'Customer_ChangedBy'),
                        $getDate($row, 'Customer_ChangeDate'), $getInt($row, 'Customer_ChangeTime'),
                        $getBool($row, 'Customer_CertOfOrigin'), $getBool($row, 'Customer_CommercialInvoice'),
                        $getBool($row, 'Customer_ShipExprtDeclartn'), $getBool($row, 'Customer_NonStdPkg'),
                        $getInt($row, 'Customer_DeliveryConf'), $getInt($row, 'Customer_TaxRoundRule'),
                        $getVal($row, 'Customer_TaxMethod'), $getBool($row, 'Customer_InvPerPackLine'),
                        $getVal($row, 'Customer_OrgRegCode'), $getBool($row, 'Customer_PeriodicBilling'),
                        $getVal($row, 'Customer_DueDateCriteria'), $getInt($row, 'Customer_PBTerms'),
                        $getInt($row, 'Customer_SysRevID'), $getVal($row, 'Customer_SysRowID'),
                        $getVal($row, 'Customer_DistrictName'), $getVal($row, 'Customer_StreetName'),
                        $getVal($row, 'Customer_BuildingNumber'), $getVal($row, 'Customer_Floor'),
                        $getVal($row, 'Customer_Room'), $getVal($row, 'Customer_PostBox'),
                        $getVal($row, 'Customer_BTDistrictName'), $getVal($row, 'Customer_BTStreetName'),
                        $getVal($row, 'Customer_BTBuildingNumber'), $getVal($row, 'Customer_BTFloor'),
                        $getVal($row, 'Customer_BTRoom'), $getVal($row, 'Customer_BTPostBox'),
                        $getVal($row, 'Customer_CreditHoldReason'), $getVal($row, 'Customer_CreditHoldNote'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO customer ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("Customer Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data Customer ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data Customer Epicor selesai.',
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
    public function fetchDataCustomer(Request $request): JsonResponse
    {
        $result = $this->syncCustomerData();

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

