<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\JobMtlController;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class SyncJobMtl extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-jobmtl {arg1? : Periode Awal (ym) atau StartDate (Ymd)} 
                            {arg2? : Periode Akhir (ym) atau StartDate (Ymd)} 
                            {arg3? : StartDate (Ymd)}
                            {--all : Ambil semua data tanpa filter Periode atau StartDate}';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data JobMtl harian dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        $controller = new JobMtlController();

        // --- SCENARIO 3: --all (Ambil Semua Data) ---
        if ($this->option('all')) {
            if ($this->argument('arg1') || $this->argument('arg2') || $this->argument('arg3')) {
                $this->error('Opsi --all tidak dapat digabungkan dengan argumen (Periode/StartDate).');
                return Command::FAILURE;
            }
            
            $this->info('Memulai sinkronisasi SEMUA data JobMtl dari Epicor');

            // Panggil controller dengan null, null
            $result = $controller->syncJobMtlData(null, null);
            
            return $this->displaySingleResult($result);
        }

        // --- Logika Argumen (untuk Skenario 1 dan 2) ---
        $arg1 = $this->argument('arg1');
        $arg2 = $this->argument('arg2');
        $arg3 = $this->argument('arg3');
        
        $period = null;
        $endPeriod = null;
        $startDate = null;
        
        // Cek argumen 3: Jika ada, itu pasti startDate
        if (!is_null($arg3)) {
            $period = $arg1;
            $endPeriod = $arg2;
            $startDate = $arg3;
        } 
        // Cek argumen 2:
        elseif (!is_null($arg2)) {
            // Format periode selalu 'ym' (4 digit) dan StartDate 'Ymd' (8 digit)
            if (strlen($arg2) === 4 && strlen($arg1) === 4) {
                $period = $arg1;
                $endPeriod = $arg2;
            } elseif (strlen($arg2) >= 6) { 
                $period = $arg1;
                $startDate = $arg2;
            } else {
                $this->error('Format argumen kedua tidak dikenali...');
                return Command::FAILURE;
            }
        } 
        // Cek argumen 1:
        elseif (!is_null($arg1)) {
            // Hanya period
            if (strlen($arg1) === 4) {
                $period = $arg1;
            } 
            // Hanya startDate
            elseif (strlen($arg1) >= 6) {
                $startDate = $arg1;
            } else {
                $this->error('Format argumen pertama tidak dikenali...');
                return Command::FAILURE;
            }
        }
        // --- Akhir Logika Parsing Argumen ---

        // --- SCENARIO 2: RANGE (Periode Awal dan Akhir diisi) ---
        if ($endPeriod) {
            if (!$period) {
                $this->error('Periode awal (argumen pertama) harus diisi jika EndPeriod diisi.');
                return Command::FAILURE;
            }

            try {
                $start = Carbon::createFromFormat('ym', $period)->startOfMonth();
                $end = Carbon::createFromFormat('ym', $endPeriod)->startOfMonth();
            } catch (\Exception $e) {
                $this->error('Format periode tidak valid. Gunakan format "ym" (contoh: 2410).');
                return Command::FAILURE;
            }

            if ($start > $end) {
                $this->error('Periode awal tidak boleh lebih besar dari Periode akhir.');
                return Command::FAILURE;
            }

            $current = $start->copy();
            $overallTotalProcessed = 0;
            $overallTotalBatches = 0;
            $successCount = 0;
            $failCount = 0;

            while ($current <= $end) {
                $periodString = $current->format('ym');
                $this->line('');
                $this->info("--- Memproses Periode: $periodString ---");

                // Panggilan ini sekarang akan mengirimkan $startDate sebagai null (jika tidak diisi)
                $result = $controller->syncJobMtlData($periodString, $startDate);

                if (!$result['success']) {
                    $errorMsg = $result['error'] ?? 'Unknown error';
                    $this->error("Sinkronisasi Gagal untuk $periodString! " . $errorMsg);
                    $failCount++;
                } else {
                    // Akumulasi total dan tampilan sukses
                    $this->info("Sinkronisasi Berhasil untuk $periodString! (Total: " . $result['total_processed_api_rows'] . " baris diproses)");
                    $overallTotalProcessed += $result['total_processed_api_rows'];
                    $overallTotalBatches += $result['total_db_batches_processed'];
                    $successCount++;
                }
                
                $current->addMonth();
            }

            $this->line('');
            $this->info('======== SINKRONISASI RANGE SELESAI ========');
            $this->info("Total Periode Sukses: $successCount");
            $this->info("Total Periode Gagal: $failCount");
            $this->info("Total Baris Diproses (Keseluruhan): $overallTotalProcessed");
            
            return $failCount > 0 ? Command::FAILURE : Command::SUCCESS;

        } 
        
        // --- SCENARIO 1: TUNGGAL (atau DEFAULT) ---
        else {
            $hasArgs = !is_null($arg1) || !is_null($arg2) || !is_null($arg3);

            if ($hasArgs) {
                 $this->info('Memulai sinkronisasi data JobMtl dari Epicor');
            } else {
                // INILAH LOGIKA DEFAULT (SCENARIO 1)
                $this->info('Menjalankan sinkronisasi untuk hari ini.');
                $startDate = date('Ymd');
                $period = date('ym', strtotime($startDate));
            }
            
            if ($period) {
                $this->info("Periode: $period");
            }
            if ($startDate) {
                $this->info("StartDate: $startDate");
            }
            
            // Panggil controller
            $result = $controller->syncJobMtlData($period, $startDate); 
            
            return $this->displaySingleResult($result); // Panggil fungsi helper
        }
    }

    /**
     * Helper untuk menampilkan hasil Mode Tunggal atau Mode --all
     * @param array $result
     * @return int
     */
    private function displaySingleResult(array $result): int
    {
        if (!$result['success']) {
            $errorMsg = $result['error'] ?? 'Unknown error';
            $this->error('Sinkronisasi Gagal! ' . $errorMsg);
            return Command::FAILURE;
        }

        $this->info('Sinkronisasi Berhasil!');
        $this->table(
            ['Metrik', 'Nilai'],
            [
                ['Total Baris Diproses', $result['total_processed_api_rows']],
                ['Total Batch Database', $result['total_db_batches_processed']],
            ]
        );

        return Command::SUCCESS;
    }
}