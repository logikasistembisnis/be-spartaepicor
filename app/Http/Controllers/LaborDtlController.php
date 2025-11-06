<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class LaborDtlController extends Controller
{
    /**
     * Mengambil data LaborDtl dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @param string|null $period Periode bulan (e.g., '2510').
     * @param string|null $startDate Tanggal mulai (YYYYMMDD).
     * @return array Hasil summary
     */
    public function syncLaborDtlData(?string $period = null, ?string $startDate = null): array
    {
        // Inisialisasi dan Konfigurasi
        $INTERNAL_BATCH_SIZE = 500;
        ini_set('memory_limit', '512M');
        set_time_limit(1800); // 30 menit

        $offsetNum = 0;
        $fetchNum = 5000;
        $totalProcessed = 0;
        $batchCount = 0;
        $maxLaborHedSeqProcessed = 0;
        $maxLaborDtlSeqProcessed = 0;

        // Definisi Kolom dan Sintaks SQL
        $columnNames = [
            'employeenum', 'laborhedseq', 'labordtlseq', 'labortype', 'labortypepseudo',
            'rework', 'reworkreasoncode', 'jobnum', 'assemblyseq', 'oprseq',
            'jcdept', 'resourcegrpid', 'opcode', 'laborhrs', 'burdenhrs',
            'laborqty', 'scrapqty', 'scrapreasoncode', 'setuppctcomplete', 'complete',
            'labornote', 'expensecode', 'clockinminute', 'clockoutminute', 'clockindate',
            'clockintime', 'clockouttime', 'activetrans', 'laborrate', 'burdenrate',
            'dspclockintime', 'dspclockouttime', 'resourceid', 'opcomplete', 'postedtogl',
            'approveddate', 'timestatus', 'createdby', 'createdate', 'createtime',
            'changedby', 'changedate', 'changetime', 'submittedby', 'shift',
            'jrtiwhcode_c', 'jrtibinnum_c', 'jrtilotnumber_c', 'onfielddifference_c',
            'jrtilotnumberprod_c', 'srcentry_c', 'cancelled_c', 'labordtlidadj_c',
            'imwhcode_c', 'imwhbinnum_c', 'imlotnum_c', 'qtyngrepairable_c', 'sysrevid', 'sysrowid'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['laborhedseq', 'labordtlseq']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'laborhedseq, labordtlseq';

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
            ->get(env('EPICOR_API_URL'). '/ETL_LaborDtl/Data', $apiParams);

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
                        ->reverse()
                        ->unique(fn($r) => $r['LaborDtl_LaborHedSeq'] . '-' . $r['LaborDtl_LaborDtlSeq'])
                        ->values()
                        ->toArray();
                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getFloat = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $laborHedSeq = $getInt($row, 'LaborDtl_LaborHedSeq');
                    $laborDtlSeq = $getInt($row, 'LaborDtl_LaborDtlSeq');

                    $rowData = [
                        $getVal($row, 'LaborDtl_EmployeeNum'), $laborHedSeq, $laborDtlSeq,
                        $getVal($row, 'LaborDtl_LaborType'), $getVal($row, 'LaborDtl_LaborTypePseudo'),
                        $getBool($row, 'LaborDtl_ReWork'), $getVal($row, 'LaborDtl_ReworkReasonCode'),
                        $getVal($row, 'LaborDtl_JobNum'), $getInt($row, 'LaborDtl_AssemblySeq'),
                        $getInt($row, 'LaborDtl_OprSeq'), $getVal($row, 'LaborDtl_JCDept'),
                        $getVal($row, 'LaborDtl_ResourceGrpID'), $getVal($row, 'LaborDtl_OpCode'),
                        $getFloat($row, 'LaborDtl_LaborHrs'), $getFloat($row, 'LaborDtl_BurdenHrs'),
                        $getFloat($row, 'LaborDtl_LaborQty'), $getFloat($row, 'LaborDtl_ScrapQty'),
                        $getVal($row, 'LaborDtl_ScrapReasonCode'), $getInt($row, 'LaborDtl_SetupPctComplete'),
                        $getBool($row, 'LaborDtl_Complete'), $getVal($row, 'LaborDtl_LaborNote'),
                        $getVal($row, 'LaborDtl_ExpenseCode'), $getInt($row, 'LaborDtl_ClockInMInute'),
                        $getInt($row, 'LaborDtl_ClockOutMinute'), $getDate($row, 'LaborDtl_ClockInDate'),
                        $getFloat($row, 'LaborDtl_ClockinTime'), $getFloat($row, 'LaborDtl_ClockOutTime'),
                        $getBool($row, 'LaborDtl_ActiveTrans'), $getFloat($row, 'LaborDtl_LaborRate'),
                        $getFloat($row, 'LaborDtl_BurdenRate'), $getVal($row, 'LaborDtl_DspClockInTime'),
                        $getVal($row, 'LaborDtl_DspClockOutTime'), $getVal($row, 'LaborDtl_ResourceID'),
                        $getBool($row, 'LaborDtl_OpComplete'), $getBool($row, 'LaborDtl_PostedToGL'),
                        $getDate($row, 'LaborDtl_ApprovedDate'), $getVal($row, 'LaborDtl_TimeStatus'),
                        $getVal($row, 'LaborDtl_CreatedBy'), $getDate($row, 'LaborDtl_CreateDate'),
                        $getInt($row, 'LaborDtl_CreateTime'), $getVal($row, 'LaborDtl_ChangedBy'),
                        $getDate($row, 'LaborDtl_ChangeDate'), $getInt($row, 'LaborDtl_ChangeTime'),
                        $getVal($row, 'LaborDtl_SubmittedBy'), $getInt($row, 'LaborDtl_Shift'),
                        $getVal($row, 'LaborDtl_JRTIWHCode_c'), $getVal($row, 'LaborDtl_JRTIBinNum_c'),
                        $getVal($row, 'LaborDtl_JRTILotnumber_c'), $getFloat($row, 'LaborDtl_OnFieldDifference_c'),
                        $getVal($row, 'LaborDtl_JRTILotNumberProd_c'), $getVal($row, 'LaborDtl_SrcEntry_c'),
                        $getBool($row, 'LaborDtl_Cancelled_c'), $getInt($row, 'LaborDtl_LaborDtlIDAdj_c'),
                        $getVal($row, 'LaborDtl_IMWHCode_c'), $getVal($row, 'LaborDtl_IMWHBinNum_c'),
                        $getVal($row, 'LaborDtl_IMLotNum_c'), $getFloat($row, 'LaborDtl_qtyngrepairable_c'),
                        $getInt($row, 'LaborDtl_SysRevID'), $getVal($row, 'LaborDtl_SysRowID'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                    $maxLaborHedSeqProcessed = max($maxLaborHedSeqProcessed, $laborHedSeq);
                    $maxLaborDtlSeqProcessed = max($maxLaborDtlSeqProcessed, $laborDtlSeq);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO labordtl ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("LaborDtl Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data LaborDtl ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data LaborDtl Epicor selesai.',
            'filter_start_date' => $startDate,
            'filter_period' => $period,
            'total_processed_api_rows' => $totalProcessed,
            'total_db_batches_processed' => $batchCount,
            'last_laborhedseq_in_data' => $maxLaborHedSeqProcessed,
            'last_labordtlseq_in_data' => $maxLaborDtlSeqProcessed,
        ];
    }
    
    /**
     * HTTP Endpoint for fetching data from Epicor API.
     *
     * @param Request $request
     * @return JsonResponse
     */
    public function fetchDataLaborDtl(Request $request): JsonResponse
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

