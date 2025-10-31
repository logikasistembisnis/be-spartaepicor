<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class UD06Controller extends Controller
{
    /**
     * Mengambil data UD06 dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncUD06Data(): array
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
            'key1','key2','key3','key4','key5',
            'character01','character02','character03','character04','character05',
            'character06','character07','character08','character09','character10',
            'number01','number02','number03','number04','number05',
            'number06','number07','number08','number09','number10',
            'number11','number12','number13','number14','number15',
            'number16','number17','number18','number19','number20',
            'date01','date02','date03','date04','date05',
            'date06','date07','date08','date09','date10',
            'date11','date12','date13','date14','date15',
            'date16','date17','date18','date19','date20',
            'checkbox01','checkbox02','checkbox03','checkbox04','checkbox05',
            'checkbox06','checkbox07','checkbox08','checkbox09','checkbox10',
            'checkbox11','checkbox12','checkbox13','checkbox14','checkbox15',
            'checkbox16','checkbox17','checkbox18','checkbox19','checkbox20',
            'shortchar01','shortchar02','shortchar03','shortchar04','shortchar05',
            'shortchar06','shortchar07','shortchar08','shortchar09','shortchar10',
            'shortchar11','shortchar12','shortchar13','shortchar14','shortchar15',
            'shortchar16','shortchar17','shortchar18','shortchar19','shortchar20',
            'globalud06','globallock','sysrevid','sysrowid',
            'partwip_c','partfg_c','wipno_c','tipeopr_c','bomlvlrevs_c',
            'nexttipeopr_c','beforescno_c','scno_c','mchno_c','mtlpart_c',
            'fgrowno_c','operation_c'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['key1','key2','key3','key4','key5']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'key1, key2, key3, key4, key5';

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_UD06/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data UD06", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data UD06 dari API',
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
                        $getVal($row, 'UD06_Key1'), $getVal($row, 'UD06_Key2'), $getVal($row, 'UD06_Key3'),
                        $getVal($row, 'UD06_Key4'), $getVal($row, 'UD06_Key5'),

                        $getVal($row, 'UD06_Character01'), $getVal($row, 'UD06_Character02'), $getVal($row, 'UD06_Character03'),
                        $getVal($row, 'UD06_Character04'), $getVal($row, 'UD06_Character05'),
                        $getVal($row, 'UD06_Character06'), $getVal($row, 'UD06_Character07'), $getVal($row, 'UD06_Character08'),
                        $getVal($row, 'UD06_Character09'), $getVal($row, 'UD06_Character10'),

                        $getNum($row, 'UD06_Number01'), $getNum($row, 'UD06_Number02'), $getNum($row, 'UD06_Number03'),
                        $getNum($row, 'UD06_Number04'), $getNum($row, 'UD06_Number05'),
                        $getNum($row, 'UD06_Number06'), $getNum($row, 'UD06_Number07'), $getNum($row, 'UD06_Number08'),
                        $getNum($row, 'UD06_Number09'), $getNum($row, 'UD06_Number10'),
                        $getNum($row, 'UD06_Number11'), $getNum($row, 'UD06_Number12'), $getNum($row, 'UD06_Number13'),
                        $getNum($row, 'UD06_Number14'), $getNum($row, 'UD06_Number15'),
                        $getNum($row, 'UD06_Number16'), $getNum($row, 'UD06_Number17'), $getNum($row, 'UD06_Number18'),
                        $getNum($row, 'UD06_Number19'), $getNum($row, 'UD06_Number20'),

                        $getDate($row, 'UD06_Date01'), $getDate($row, 'UD06_Date02'), $getDate($row, 'UD06_Date03'),
                        $getDate($row, 'UD06_Date04'), $getDate($row, 'UD06_Date05'),
                        $getDate($row, 'UD06_Date06'), $getDate($row, 'UD06_Date07'), $getDate($row, 'UD06_Date08'),
                        $getDate($row, 'UD06_Date09'), $getDate($row, 'UD06_Date10'),
                        $getDate($row, 'UD06_Date11'), $getDate($row, 'UD06_Date12'), $getDate($row, 'UD06_Date13'),
                        $getDate($row, 'UD06_Date14'), $getDate($row, 'UD06_Date15'),
                        $getDate($row, 'UD06_Date16'), $getDate($row, 'UD06_Date17'), $getDate($row, 'UD06_Date18'),
                        $getDate($row, 'UD06_Date19'), $getDate($row, 'UD06_Date20'),

                        $getBool($row, 'UD06_CheckBox01'), $getBool($row, 'UD06_CheckBox02'), $getBool($row, 'UD06_CheckBox03'),
                        $getBool($row, 'UD06_CheckBox04'), $getBool($row, 'UD06_CheckBox05'),
                        $getBool($row, 'UD06_CheckBox06'), $getBool($row, 'UD06_CheckBox07'), $getBool($row, 'UD06_CheckBox08'),
                        $getBool($row, 'UD06_CheckBox09'), $getBool($row, 'UD06_CheckBox10'),
                        $getBool($row, 'UD06_CheckBox11'), $getBool($row, 'UD06_CheckBox12'), $getBool($row, 'UD06_CheckBox13'),
                        $getBool($row, 'UD06_CheckBox14'), $getBool($row, 'UD06_CheckBox15'),
                        $getBool($row, 'UD06_CheckBox16'), $getBool($row, 'UD06_CheckBox17'), $getBool($row, 'UD06_CheckBox18'),
                        $getBool($row, 'UD06_CheckBox19'), $getBool($row, 'UD06_CheckBox20'),

                        $getVal($row, 'UD06_ShortChar01'), $getVal($row, 'UD06_ShortChar02'), $getVal($row, 'UD06_ShortChar03'),
                        $getVal($row, 'UD06_ShortChar04'), $getVal($row, 'UD06_ShortChar05'),
                        $getVal($row, 'UD06_ShortChar06'), $getVal($row, 'UD06_ShortChar07'), $getVal($row, 'UD06_ShortChar08'),
                        $getVal($row, 'UD06_ShortChar09'), $getVal($row, 'UD06_ShortChar10'),
                        $getVal($row, 'UD06_ShortChar11'), $getVal($row, 'UD06_ShortChar12'), $getVal($row, 'UD06_ShortChar13'),
                        $getVal($row, 'UD06_ShortChar14'), $getVal($row, 'UD06_ShortChar15'),
                        $getVal($row, 'UD06_ShortChar16'), $getVal($row, 'UD06_ShortChar17'), $getVal($row, 'UD06_ShortChar18'),
                        $getVal($row, 'UD06_ShortChar19'), $getVal($row, 'UD06_ShortChar20'),

                        $getBool($row, 'UD06_GlobalUD06'), $getBool($row, 'UD06_GlobalLock'),
                        $getInt($row, 'UD06_SysRevID'), $getVal($row, 'UD06_SysRowID'),

                        $getVal($row, 'UD06_partwip_c'), $getVal($row, 'UD06_partfg_c'), $getInt($row, 'UD06_wipno_c'),
                        $getVal($row, 'UD06_tipeopr_c'), $getInt($row, 'UD06_bomlvlrevs_c'),
                        $getVal($row, 'UD06_nexttipeopr_c'), $getInt($row, 'UD06_beforescno_c'), $getInt($row, 'UD06_scno_c'),
                        $getInt($row, 'UD06_mchno_c'), $getVal($row, 'UD06_mtlpart_c'),
                        $getInt($row, 'UD06_FGRowNo_c'), $getVal($row, 'UD06_operation_c')
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO ud06 ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("UD06 Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data UD06 ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data UD06 Epicor selesai.',
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
    public function fetchDataUD06(Request $request): JsonResponse
    {
        $result = $this->syncUD06Data();

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

