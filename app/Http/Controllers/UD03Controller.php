<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class UD03Controller extends Controller
{
    /**
     * Mengambil data UD03 dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncUD03Data(?string $period = null, ?string $startDate = null): array
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
            'sysrevid','sysrowid',
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['key1','key2','key3','key4','key5']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'key1, key2, key3, key4, key5';

        do {
            $apiParams = [
                'OffsetNum' => (string)$offsetNum,
                'FetchNum' => (string)$fetchNum,
                'Periode' => (string)$period,
                'StartDate' => (string)$startDate, // null akan menjadi ""
            ];

            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_UD03/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data UD03", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data UD03 dari API',
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
                        $getVal($row, 'UD03_Key1'), $getVal($row, 'UD03_Key2'), $getVal($row, 'UD03_Key3'),
                        $getVal($row, 'UD03_Key4'), $getVal($row, 'UD03_Key5'),

                        $getVal($row, 'UD03_Character01'), $getVal($row, 'UD03_Character02'), $getVal($row, 'UD03_Character03'),
                        $getVal($row, 'UD03_Character04'), $getVal($row, 'UD03_Character05'), $getVal($row, 'UD03_Character06'),
                        $getVal($row, 'UD03_Character07'), $getVal($row, 'UD03_Character08'), $getVal($row, 'UD03_Character09'),
                        $getVal($row, 'UD03_Character10'),

                        $getNum($row, 'UD03_Number01'), $getNum($row, 'UD03_Number02'), $getNum($row, 'UD03_Number03'),
                        $getNum($row, 'UD03_Number04'), $getNum($row, 'UD03_Number05'),
                        $getNum($row, 'UD03_Number06'), $getNum($row, 'UD03_Number07'), $getNum($row, 'UD03_Number08'),
                        $getNum($row, 'UD03_Number09'), $getNum($row, 'UD03_Number10'),
                        $getNum($row, 'UD03_Number11'), $getNum($row, 'UD03_Number12'), $getNum($row, 'UD03_Number13'),
                        $getNum($row, 'UD03_Number14'), $getNum($row, 'UD03_Number15'),
                        $getNum($row, 'UD03_Number16'), $getNum($row, 'UD03_Number17'), $getNum($row, 'UD03_Number18'),
                        $getNum($row, 'UD03_Number19'), $getNum($row, 'UD03_Number20'),

                        $getDate($row, 'UD03_Date01'), $getDate($row, 'UD03_Date02'), $getDate($row, 'UD03_Date03'),
                        $getDate($row, 'UD03_Date04'), $getDate($row, 'UD03_Date05'),
                        $getDate($row, 'UD03_Date06'), $getDate($row, 'UD03_Date07'), $getDate($row, 'UD03_Date08'),
                        $getDate($row, 'UD03_Date09'), $getDate($row, 'UD03_Date10'),
                        $getDate($row, 'UD03_Date11'), $getDate($row, 'UD03_Date12'), $getDate($row, 'UD03_Date13'),
                        $getDate($row, 'UD03_Date14'), $getDate($row, 'UD03_Date15'),
                        $getDate($row, 'UD03_Date16'), $getDate($row, 'UD03_Date17'), $getDate($row, 'UD03_Date18'),
                        $getDate($row, 'UD03_Date19'), $getDate($row, 'UD03_Date20'),

                        $getBool($row, 'UD03_CheckBox01'), $getBool($row, 'UD03_CheckBox02'), $getBool($row, 'UD03_CheckBox03'),
                        $getBool($row, 'UD03_CheckBox04'), $getBool($row, 'UD03_CheckBox05'),
                        $getBool($row, 'UD03_CheckBox06'), $getBool($row, 'UD03_CheckBox07'), $getBool($row, 'UD03_CheckBox08'),
                        $getBool($row, 'UD03_CheckBox09'), $getBool($row, 'UD03_CheckBox10'),
                        $getBool($row, 'UD03_CheckBox11'), $getBool($row, 'UD03_CheckBox12'), $getBool($row, 'UD03_CheckBox13'),
                        $getBool($row, 'UD03_CheckBox14'), $getBool($row, 'UD03_CheckBox15'),
                        $getBool($row, 'UD03_CheckBox16'), $getBool($row, 'UD03_CheckBox17'), $getBool($row, 'UD03_CheckBox18'),
                        $getBool($row, 'UD03_CheckBox19'), $getBool($row, 'UD03_CheckBox20'),

                        $getVal($row, 'UD03_ShortChar01'), $getVal($row, 'UD03_ShortChar02'), $getVal($row, 'UD03_ShortChar03'),
                        $getVal($row, 'UD03_ShortChar04'), $getVal($row, 'UD03_ShortChar05'),
                        $getVal($row, 'UD03_ShortChar06'), $getVal($row, 'UD03_ShortChar07'), $getVal($row, 'UD03_ShortChar08'),
                        $getVal($row, 'UD03_ShortChar09'), $getVal($row, 'UD03_ShortChar10'),
                        $getVal($row, 'UD03_ShortChar11'), $getVal($row, 'UD03_ShortChar12'), $getVal($row, 'UD03_ShortChar13'),
                        $getVal($row, 'UD03_ShortChar14'), $getVal($row, 'UD03_ShortChar15'),
                        $getVal($row, 'UD03_ShortChar16'), $getVal($row, 'UD03_ShortChar17'), $getVal($row, 'UD03_ShortChar18'),
                        $getVal($row, 'UD03_ShortChar19'), $getVal($row, 'UD03_ShortChar20'),

                        $getInt($row, 'UD03_SysRevID'), $getVal($row, 'UD03_SysRowID'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO ud03 ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("UD03 Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data UD03 ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data UD03 Epicor selesai.',
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
    public function fetchDataUD03(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncUD03Data($period, $startDate);

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

