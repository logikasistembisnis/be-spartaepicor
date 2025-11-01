<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;

class DeleteRecordController extends Controller
{
    public function syncDeletedRecords(): array
    {
        $INTERNAL_BATCH_SIZE = 500;
        $offsetNum = 0;
        $fetchNum = 5000;
        $totalDeleted = 0;
        $totalProcessed = 0;

        // Hitung timestamp 6 jam sebelum sekarang
        $epochTime = strtotime('-6 hours');

        // Daftar tabel yang boleh dihapus
        $allowedTables = [
            'parttran',
            'labordtl',
            'rcvdtl',
            'jobhead',
            'jobmtl',
            'opmaster',
            'part',
            'partclass',
            'podetail',
            'poheader',
            'porel',
            'rcvhead',
            'resource',
            'ud06',
            'ud11',
            'ud101',
            'warehouse',
            'warehousebin',
        ];

        do {
            $response = Http::withHeaders([
                'x-api-key' => env('EPICOR_API_KEY'),
                'License' => env('EPICOR_LICENSE'),
            ])->withBasicAuth(env('EPICOR_USERNAME'), env('EPICOR_PASSWORD'))
            ->timeout(600)
            ->get(env('EPICOR_API_URL'). '/ETL_DeleteRec/Data', [
                'OffsetNum' => $offsetNum,
                'FetchNum' => $fetchNum,
                'EpochTimeStamp' => $epochTime
            ]);

            if ($response->failed()) {
                $status = $response->status();
                $error = $response->body();
                Log::error("Gagal ambil data delete record", ['status' => $status, 'body' => $error]);
                return [
                    'success' => false,
                    'error' => 'Gagal ambil data dari API DeleteRec',
                    'status_code' => $status,
                ];
            }

            $data = $response->json()['value'] ?? [];
            $currentBatchSize = count($data);
            if ($currentBatchSize === 0) break;

            foreach ($data as $row) {
                $tableName = strtolower($row['UD14_Key4'] ?? '');
                $sysRowId  = $row['UD14_Key1'] ?? null;

                if (!$tableName || !$sysRowId) continue;
                if (!in_array($tableName, $allowedTables)) continue;

                try {
                    $deleted = DB::delete("DELETE FROM {$tableName} WHERE sysrowid = ?", [$sysRowId]);
                    $totalDeleted += $deleted;
                    $totalProcessed++;
                } catch (\Exception $e) {
                    Log::error("Gagal hapus record dari tabel $tableName", [
                        'sysrowid' => $sysRowId,
                        'error' => $e->getMessage()
                    ]);
                }
            }

            $offsetNum += $currentBatchSize;
        } while ($currentBatchSize === $fetchNum);

        return [
            'success' => true,
            'message' => 'Sinkronisasi data delete record selesai',
            'total_processed' => $totalProcessed,
            'total_deleted' => $totalDeleted,
            'epoch_timestamp_used' => $epochTime
        ];
    }

    public function fetchDeletedRecords(Request $request): JsonResponse
    {
        $result = $this->syncDeletedRecords();

        if (!$result['success']) {
            return response()->json([
                'message' => 'Gagal sinkronisasi data delete record',
                'error' => $result['error'] ?? null,
            ], $result['status_code'] ?? 500);
        }

        unset($result['success']);
        return response()->json($result, 200);
    }
}
