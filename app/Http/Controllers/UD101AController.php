<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class UD101AController extends Controller
{
    /**
     * Mengambil data UD101A dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncUD101AData(): array
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
            'childkey1', 'childkey2', 'childkey3', 'childkey4', 'childkey5',
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
            'sysrevid','sysrowid',
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
            ->get(env('EPICOR_API_URL'). '/ETL_UD101A/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data UD101A", ['status' => $status, 'body' => $errorBody]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data UD101A dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $chunk = collect($chunk)
                ->reverse() // urutkan dari yang paling lama → terbaru
                ->unique(fn($r) =>
                    trim(($r['UD101A_Key1'] ?? '') . '|' .
                        ($r['UD101A_Key2'] ?? '') . '|' .
                        ($r['UD101A_Key3'] ?? '') . '|' .
                        ($r['UD101A_Key4'] ?? '') . '|' .
                        ($r['UD101A_Key5'] ?? ''))
                )
                ->values()
                ->toArray();

                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'UD101A_Key1'), $getVal($row, 'UD101A_Key2'), $getVal($row, 'UD101A_Key3'),
                        $getVal($row, 'UD101A_Key4'), $getVal($row, 'UD101A_Key5'),

                        $getVal($row, 'UD101A_ChildKey1'), $getVal($row, 'UD101A_ChildKey2'), $getVal($row, 'UD101A_ChildKey3'),
                        $getVal($row, 'UD101A_ChildKey4'), $getVal($row, 'UD101A_ChildKey5'),

                        $getVal($row, 'UD101A_Character01'), $getVal($row, 'UD101A_Character02'), $getVal($row, 'UD101A_Character03'),
                        $getVal($row, 'UD101A_Character04'), $getVal($row, 'UD101A_Character05'),
                        $getVal($row, 'UD101A_Character06'), $getVal($row, 'UD101A_Character07'), $getVal($row, 'UD101A_Character08'),
                        $getVal($row, 'UD101A_Character09'), $getVal($row, 'UD101A_Character10'),

                        $getNum($row, 'UD101A_Number01'), $getNum($row, 'UD101A_Number02'), $getNum($row, 'UD101A_Number03'),
                        $getNum($row, 'UD101A_Number04'), $getNum($row, 'UD101A_Number05'),
                        $getNum($row, 'UD101A_Number06'), $getNum($row, 'UD101A_Number07'), $getNum($row, 'UD101A_Number08'),
                        $getNum($row, 'UD101A_Number09'), $getNum($row, 'UD101A_Number10'),
                        $getNum($row, 'UD101A_Number11'), $getNum($row, 'UD101A_Number12'), $getNum($row, 'UD101A_Number13'),
                        $getNum($row, 'UD101A_Number14'), $getNum($row, 'UD101A_Number15'),
                        $getNum($row, 'UD101A_Number16'), $getNum($row, 'UD101A_Number17'), $getNum($row, 'UD101A_Number18'),
                        $getNum($row, 'UD101A_Number19'), $getNum($row, 'UD101A_Number20'),

                        $getDate($row, 'UD101A_Date01'), $getDate($row, 'UD101A_Date02'), $getDate($row, 'UD101A_Date03'),
                        $getDate($row, 'UD101A_Date04'), $getDate($row, 'UD101A_Date05'),
                        $getDate($row, 'UD101A_Date06'), $getDate($row, 'UD101A_Date07'), $getDate($row, 'UD101A_Date08'),
                        $getDate($row, 'UD101A_Date09'), $getDate($row, 'UD101A_Date10'),
                        $getDate($row, 'UD101A_Date11'), $getDate($row, 'UD101A_Date12'), $getDate($row, 'UD101A_Date13'),
                        $getDate($row, 'UD101A_Date14'), $getDate($row, 'UD101A_Date15'),
                        $getDate($row, 'UD101A_Date16'), $getDate($row, 'UD101A_Date17'), $getDate($row, 'UD101A_Date18'),
                        $getDate($row, 'UD101A_Date19'), $getDate($row, 'UD101A_Date20'),

                        $getBool($row, 'UD101A_CheckBox01'), $getBool($row, 'UD101A_CheckBox02'), $getBool($row, 'UD101A_CheckBox03'),
                        $getBool($row, 'UD101A_CheckBox04'), $getBool($row, 'UD101A_CheckBox05'),
                        $getBool($row, 'UD101A_CheckBox06'), $getBool($row, 'UD101A_CheckBox07'), $getBool($row, 'UD101A_CheckBox08'),
                        $getBool($row, 'UD101A_CheckBox09'), $getBool($row, 'UD101A_CheckBox10'),
                        $getBool($row, 'UD101A_CheckBox11'), $getBool($row, 'UD101A_CheckBox12'), $getBool($row, 'UD101A_CheckBox13'),
                        $getBool($row, 'UD101A_CheckBox14'), $getBool($row, 'UD101A_CheckBox15'),
                        $getBool($row, 'UD101A_CheckBox16'), $getBool($row, 'UD101A_CheckBox17'), $getBool($row, 'UD101A_CheckBox18'),
                        $getBool($row, 'UD101A_CheckBox19'), $getBool($row, 'UD101A_CheckBox20'),

                        $getVal($row, 'UD101A_ShortChar01'), $getVal($row, 'UD101A_ShortChar02'), $getVal($row, 'UD101A_ShortChar03'),
                        $getVal($row, 'UD101A_ShortChar04'), $getVal($row, 'UD101A_ShortChar05'),
                        $getVal($row, 'UD101A_ShortChar06'), $getVal($row, 'UD101A_ShortChar07'), $getVal($row, 'UD101A_ShortChar08'),
                        $getVal($row, 'UD101A_ShortChar09'), $getVal($row, 'UD101A_ShortChar10'),

                        $getInt($row, 'UD101A_SysRevID'), $getVal($row, 'UD101A_SysRowID'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO ud101a ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("UD101A Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data UD101A ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data UD101A Epicor selesai.',
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
    public function fetchDataUD101A(Request $request): JsonResponse
    {
        $result = $this->syncUD101AData();

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

