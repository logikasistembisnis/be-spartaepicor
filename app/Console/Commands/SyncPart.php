<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\PartController;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

class SyncPart extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-part {arg1? : Periode Awal (ym) atau StartDate (Ymd)} 
                            {arg2? : Periode Akhir (ym) atau StartDate (Ymd)} 
                            {arg3? : StartDate (Ymd)}';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data Part dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        $arg1 = $this->argument('arg1');
        $arg2 = $this->argument('arg2');
        $arg3 = $this->argument('arg3');
        
        $period = null;
        $endPeriod = null;
        $startDate = null;
        
        // Cek argumen 3: Jika ada, itu pasti startDate.
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
            } elseif (strlen($arg2) >= 6) { // StartDate >= 6 digit (Ymd)
                $period = $arg1;
                $startDate = $arg2;
            } else {
                $this->error('Format argumen kedua tidak dikenali. Gunakan "ym" untuk EndPeriod atau "Ymd" untuk StartDate.');
                return Command::FAILURE;
            }
        } 
        // Cek argumen 1:
        elseif (!is_null($arg1)) {
            // Hanya Periode
            if (strlen($arg1) === 4) {
                $period = $arg1;
            } 
            // Hanya StartDate
            elseif (strlen($arg1) >= 6) {
                $startDate = $arg1;
            } else {
                $this->error('Format argumen pertama tidak dikenali. Gunakan "ym" untuk Periode atau "Ymd" untuk StartDate.');
                return Command::FAILURE;
            }
        }

        $controller = new PartController();

        if ($endPeriod) {
            // --- LOGIKA LOOPING (RANGE) ---
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

            $this->info("Memulai sinkronisasi Part dari periode $period sampai $endPeriod.");
            if ($startDate) {
                $this->warn("Menggunakan filter StartDate: $startDate untuk setiap periode.");
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

                $result = $controller->syncPartData($periodString, $startDate);

                if (!$result['success']) {
                    $errorMsg = $result['error'] ?? 'Unknown error';
                    $this->error("Sinkronisasi Gagal untuk $periodString! " . $errorMsg);
                    $failCount++;
                } else {
                    // Akumulasi total dan tampilan sukses
                    $this->info("Sinkronisasi Berhasil untuk $periodString!");
                    $overallTotalProcessed += $result['total_processed_api_rows'];
                    $overallTotalBatches += $result['total_db_batches_processed'];
                    $successCount++;
                }
                
                $current->addMonth();
            }

            $this->line('');
            $this->info('======== SINKRONISASI RANGE SELESAI ========');
            return $failCount > 0 ? Command::FAILURE : Command::SUCCESS;

        } 
        
        // --- LOGIKA TUNGGAL ---
        else {
            $this->info('Memulai sinkronisasi data Part dari Epicor (Mode Tunggal)');
            
            // Jika tidak ada argumen sama sekali, Controller akan menentukan periode & tanggal hari ini
            if ($period) {
                $this->info("Periode: $period");
            }
            if ($startDate) {
                $this->info("StartDate: $startDate");
            }
            
            $result = $controller->syncPartData($period, $startDate); 

            if (!$result['success']) {
                $errorMsg = $result['error'] ?? 'Unknown error';
                $this->error('Sinkronisasi Gagal! ' . $errorMsg);
                return Command::FAILURE;
            }

            $this->info('Sinkronisasi Berhasil!');
            $this->table(
                ['Metrik', 'Nilai'],
                [
                    ['Filter StartDate', $result['filter_start_date']],
                    ['Filter Period', $result['filter_period']],
                    ['Total Baris Diproses', $result['total_processed_api_rows']],
                    ['Total Batch Database', $result['total_db_batches_processed']],
                ]
            );

            return Command::SUCCESS;
        }
    }
}