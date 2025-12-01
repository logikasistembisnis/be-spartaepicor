<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class UD10Controller extends Controller
{
    /**
     * Mengambil data UD10 dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @return array Hasil summary
     */
    public function syncUD10Data(?string $period = null, ?string $startDate = null): array
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
            'sysrevid','sysrowid','calculated_changedate'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['key1','key2','key3','key4','key5']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'key1, key2, key3, key4,key5';

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
            ->get(env('EPICOR_API_URL'). '/ETL_UD10/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data UD10", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data UD10 dari API',
                    'status_code' => $status, 'details' => json_decode($errorBody, true) ?? $errorBody
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;
            
            $dataChunks = array_chunk($data, $INTERNAL_BATCH_SIZE);
            foreach ($dataChunks as $chunk) {
                $chunk = collect($chunk)
                ->reverse()
                ->unique(fn($r) => $r['UD10_Key1'] . '-' . $r['UD10_Key2'] . '-' . $r['UD10_Key3'] . '-' . $r['UD10_Key4'] . '-' . $r['UD10_Key5'])
                ->values()
                ->toArray();
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? (new Carbon($row[$key]))->format('Y-m-d H:i:s') : null;
                $getNum = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';

                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getVal($row, 'UD10_Key1'), $getVal($row, 'UD10_Key2'), $getVal($row, 'UD10_Key3'),
                        $getVal($row, 'UD10_Key4'), $getVal($row, 'UD10_Key5'),

                        $getVal($row, 'UD10_Character01'), $getVal($row, 'UD10_Character02'), $getVal($row, 'UD10_Character03'),
                        $getVal($row, 'UD10_Character04'), $getVal($row, 'UD10_Character05'), $getVal($row, 'UD10_Character06'),
                        $getVal($row, 'UD10_Character07'), $getVal($row, 'UD10_Character08'), $getVal($row, 'UD10_Character09'),
                        $getVal($row, 'UD10_Character10'),

                        $getNum($row, 'UD10_Number01'), $getNum($row, 'UD10_Number02'), $getNum($row, 'UD10_Number03'),
                        $getNum($row, 'UD10_Number04'), $getNum($row, 'UD10_Number05'),
                        $getNum($row, 'UD10_Number06'), $getNum($row, 'UD10_Number07'), $getNum($row, 'UD10_Number08'),
                        $getNum($row, 'UD10_Number09'), $getNum($row, 'UD10_Number10'),
                        $getNum($row, 'UD10_Number11'), $getNum($row, 'UD10_Number12'), $getNum($row, 'UD10_Number13'),
                        $getNum($row, 'UD10_Number14'), $getNum($row, 'UD10_Number15'),
                        $getNum($row, 'UD10_Number16'), $getNum($row, 'UD10_Number17'), $getNum($row, 'UD10_Number18'),
                        $getNum($row, 'UD10_Number19'), $getNum($row, 'UD10_Number20'),

                        $getDate($row, 'UD10_Date01'), $getDate($row, 'UD10_Date02'), $getDate($row, 'UD10_Date03'),
                        $getDate($row, 'UD10_Date04'), $getDate($row, 'UD10_Date05'),
                        $getDate($row, 'UD10_Date06'), $getDate($row, 'UD10_Date07'), $getDate($row, 'UD10_Date08'),
                        $getDate($row, 'UD10_Date09'), $getDate($row, 'UD10_Date10'),
                        $getDate($row, 'UD10_Date11'), $getDate($row, 'UD10_Date12'), $getDate($row, 'UD10_Date13'),
                        $getDate($row, 'UD10_Date14'), $getDate($row, 'UD10_Date15'),
                        $getDate($row, 'UD10_Date16'), $getDate($row, 'UD10_Date17'), $getDate($row, 'UD10_Date18'),
                        $getDate($row, 'UD10_Date19'), $getDate($row, 'UD10_Date20'),

                        $getBool($row, 'UD10_CheckBox01'), $getBool($row, 'UD10_CheckBox02'), $getBool($row, 'UD10_CheckBox03'),
                        $getBool($row, 'UD10_CheckBox04'), $getBool($row, 'UD10_CheckBox05'),
                        $getBool($row, 'UD10_CheckBox06'), $getBool($row, 'UD10_CheckBox07'), $getBool($row, 'UD10_CheckBox08'),
                        $getBool($row, 'UD10_CheckBox09'), $getBool($row, 'UD10_CheckBox10'),
                        $getBool($row, 'UD10_CheckBox11'), $getBool($row, 'UD10_CheckBox12'), $getBool($row, 'UD10_CheckBox13'),
                        $getBool($row, 'UD10_CheckBox14'), $getBool($row, 'UD10_CheckBox15'),
                        $getBool($row, 'UD10_CheckBox16'), $getBool($row, 'UD10_CheckBox17'), $getBool($row, 'UD10_CheckBox18'),
                        $getBool($row, 'UD10_CheckBox19'), $getBool($row, 'UD10_CheckBox20'),

                        $getVal($row, 'UD10_ShortChar01'), $getVal($row, 'UD10_ShortChar02'), $getVal($row, 'UD10_ShortChar03'),
                        $getVal($row, 'UD10_ShortChar04'), $getVal($row, 'UD10_ShortChar05'),
                        $getVal($row, 'UD10_ShortChar06'), $getVal($row, 'UD10_ShortChar07'), $getVal($row, 'UD10_ShortChar08'),
                        $getVal($row, 'UD10_ShortChar09'), $getVal($row, 'UD10_ShortChar10'),
                        $getVal($row, 'UD10_ShortChar11'), $getVal($row, 'UD10_ShortChar12'), $getVal($row, 'UD10_ShortChar13'),
                        $getVal($row, 'UD10_ShortChar14'), $getVal($row, 'UD10_ShortChar15'),
                        $getVal($row, 'UD10_ShortChar16'), $getVal($row, 'UD10_ShortChar17'), $getVal($row, 'UD10_ShortChar18'),
                        $getVal($row, 'UD10_ShortChar19'), $getVal($row, 'UD10_ShortChar20'),

                        $getInt($row, 'UD10_SysRevID'), $getVal($row, 'UD10_SysRowID'), $getDate($row, 'Calculated_ChangeDate'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO ud10 ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("UD10 Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data UD10 ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data UD10 Epicor selesai.',
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
    public function fetchDataUD10(Request $request): JsonResponse
    {
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        $result = $this->syncUD10Data($period, $startDate);

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

