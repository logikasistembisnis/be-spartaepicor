<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class JobHeadController extends Controller
{
    /**
     * Mengambil data JobHead dari API Epicor (dengan paginasi) dan melakukan UPSERT.
     * Fungsi ini akan dipanggil oleh Artisan Command.
     *
     * @param string|null $period Periode bulan (e.g., '2510').
     * @param string|null $startDate Tanggal mulai (YYYYMMDD).
     * @return array Hasil summary
     */
    public function syncJobHeadData(?string $period = null, ?string $startDate = null): array
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
            'jobclosed', 'closeddate', 'jobcomplete', 'jobcompletiondate', 'jobengineered', 'jobreleased', 
            'jobheld', 'schedstatus', 'jobnum', 'partnum', 'revisionnum', 'partdescription', 
            'prodqty', 'ium', 'startdate', 'starthour', 'duedate', 'duehour', 'reqduedate', 'jobcode', 
            'quotenum', 'quoteline', 'prodcode', 'commenttext', 'expensecode', 'incopylist', 'winame', 
            'wistartdate', 'wistarthour', 'widuedate', 'widuehour', 'candidate', 'schedcode', 'schedlocked', 
            'wipcleared', 'jobfirm', 'personlist', 'personid', 'prodteamid', 'qtycompleted', 'plant', 
            'travelerreadytoprint', 'statusreadytoprint', 'callnum', 'callline', 'jobtype', 'lockqty', 
            'plannedactiondate', 'plannedkitdate', 'productionyield', 'origprodqty', 'preserveorigqtys', 
            'createdby', 'createdate', 'whseallocflag', 'equipid', 'plannum', 'maintpriority', 'splitjob', 
            'numbersource', 'schedseq', 'groupseq', 'roughcut', 'sysrevid', 'sysrowid', 'dayslate', 'contractid', 
            'readytofulfill', 'personidname'
        ];
        $columnsSql = implode(', ', $columnNames);
        $numColumns = count($columnNames);
        $placeholderRow = '(' . implode(', ', array_fill(0, $numColumns, '?')) . ')';
        $updateColumns = array_filter($columnNames, fn($col) => !in_array($col, ['jobnum']));
        $updateSetSql = implode(', ', array_map(fn($col) => "{$col} = EXCLUDED.{$col}", $updateColumns));
        $conflictKeys = 'jobnum';

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
            ->get(env('EPICOR_API_URL'). '/ETL_JobHead/Data', $apiParams);

            if ($response->failed()) {
                $status = $response->status();
                $errorBody = $response->body();
                Log::error("Gagal mengambil data JobHead", ['status' => $status, 'body' => $errorBody, 'period' => $period, 'start_date' => $startDate]);
                return [
                    'success' => false, 'error' => 'Gagal ambil data JobHead dari API',
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
                    ->unique(fn($r) => $r['JobHead_JobNum'])
                    ->values()
                    ->toArray();

                $getVal = fn($row, $key, $default = null) => $row[$key] ?? $default;
                $getDate = fn($row, $key) => isset($row[$key]) ? substr($row[$key], 0, 10) : null;
                $getFloat = fn($row, $key, $default = 0.0) => (float)($row[$key] ?? $default);
                $getInt = fn($row, $key, $default = 0) => (int)($row[$key] ?? $default);
                $getBool = fn($row, $key) => (bool)($row[$key] ?? false) ? '1' : '0';
                
                $currentChunkBindValues = [];

                foreach ($chunk as $row) {
                    $rowData = [
                        $getBool($row, 'JobHead_JobClosed'), $getDate($row, 'JobHead_ClosedDate'),
                        $getBool($row, 'JobHead_JobComplete'), $getDate($row, 'JobHead_JobCompletionDate'),
                        $getBool($row, 'JobHead_JobEngineered'), $getBool($row, 'JobHead_JobReleased'),
                        $getBool($row, 'JobHead_JobHeld'), $getVal($row, 'JobHead_SchedStatus'),
                        $getVal($row, 'JobHead_JobNum'), $getVal($row, 'JobHead_PartNum'),
                        $getVal($row, 'JobHead_RevisionNum'), 
                        $getVal($row, 'JobHead_PartDescription'), $getFloat($row, 'JobHead_ProdQty'),
                        $getVal($row, 'JobHead_IUM'), $getDate($row, 'JobHead_StartDate'),
                        $getFloat($row, 'JobHead_StartHour'), $getDate($row, 'JobHead_DueDate'),
                        $getFloat($row, 'JobHead_DueHour'), $getDate($row, 'JobHead_ReqDueDate'),
                        $getVal($row, 'JobHead_JobCode'), $getInt($row, 'JobHead_QuoteNum'),
                        $getInt($row, 'JobHead_QuoteLine'), $getVal($row, 'JobHead_ProdCode'),
                        $getVal($row, 'JobHead_CommentText'), $getVal($row, 'JobHead_ExpenseCode'),
                        $getBool($row, 'JobHead_InCopyList'), $getVal($row, 'JobHead_WIName'),
                        $getDate($row, 'JobHead_WIStartDate'), $getFloat($row, 'JobHead_WIStartHour'),
                        $getDate($row, 'JobHead_WIDueDate'), $getFloat($row, 'JobHead_WIDueHour'),
                        $getBool($row, 'JobHead_Candidate'), $getVal($row, 'JobHead_SchedCode'),
                        $getBool($row, 'JobHead_SchedLocked'), $getBool($row, 'JobHead_WIPCleared'),
                        $getBool($row, 'JobHead_JobFirm'), $getVal($row, 'JobHead_PersonList'),
                        $getVal($row, 'JobHead_PersonID'), $getVal($row, 'JobHead_ProdTeamID'),
                        $getFloat($row, 'JobHead_QtyCompleted'), $getVal($row, 'JobHead_Plant'),
                        $getBool($row, 'JobHead_TravelerReadyToPrint'), $getBool($row, 'JobHead_StatusReadyToPrint'),
                        $getInt($row, 'JobHead_CallNum'), $getInt($row, 'JobHead_CallLine'),
                        $getVal($row, 'JobHead_JobType'), $getBool($row, 'JobHead_LockQty'),
                        $getDate($row, 'JobHead_PlannedActionDate'), $getDate($row, 'JobHead_PlannedKitDate'),
                        $getBool($row, 'JobHead_ProductionYield'), $getFloat($row, 'JobHead_OrigProdQty'),
                        $getBool($row, 'JobHead_PreserveOrigQtys'), $getVal($row, 'JobHead_CreatedBy'),
                        $getDate($row, 'JobHead_CreateDate'), $getBool($row, 'JobHead_WhseAllocFlag'),
                        $getVal($row, 'JobHead_EquipID'), $getInt($row, 'JobHead_PlanNum'),
                        $getVal($row, 'JobHead_MaintPriority'), $getBool($row, 'JobHead_SplitJob'),
                        $getBool($row, 'JobHead_NumberSource'), $getInt($row, 'JobHead_SchedSeq'),
                        $getInt($row, 'JobHead_GroupSeq'), $getFloat($row, 'JobHead_RoughCut'),
                        $getInt($row, 'JobHead_SysRevID'), $getVal($row, 'JobHead_SysRowID'),
                        $getInt($row, 'JobHead_DaysLate'), $getVal($row, 'JobHead_ContractID'),
                        $getBool($row, 'JobHead_ReadyToFulfill'), $getVal($row, 'JobHead_PersonIDName'),
                    ];

                    array_push($currentChunkBindValues, ...$rowData);
                }
                
                if (!empty($currentChunkBindValues)) {
                    try {
                        DB::beginTransaction();
                        $placeholderRows = implode(', ', array_fill(0, count($chunk), $placeholderRow));
                        $sql = "INSERT INTO jobhead ({$columnsSql}) VALUES {$placeholderRows} ON CONFLICT ({$conflictKeys}) DO UPDATE SET {$updateSetSql}";
                        DB::insert($sql, $currentChunkBindValues);
                        DB::commit();
                        $batchCount++;
                        $totalProcessed += count($chunk);
                    } catch (\Exception $e) {
                        DB::rollBack();
                        Log::error("JobHead Batch UPSERT Gagal", ['error' => $e->getMessage()]);
                        return [
                            'success' => false, 'error' => 'Gagal memasukkan data JobHead ke database.', 
                            'details' => $e->getMessage(), 'status_code' => 500
                        ];
                    }
                }
            } 
            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data JobHead Epicor selesai.',
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
    public function fetchDataJobHead(Request $request): JsonResponse
    {
        // Ambil parameter dari query string (bisa null)
        $period = $request->query('period');
        $startDate = $request->query('startDate');

        // Panggil fungsi inti. 
        $result = $this->syncJobHeadData($period, $startDate);

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

