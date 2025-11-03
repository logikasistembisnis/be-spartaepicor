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
    protected $signature = 'sync:epicor-part {startPeriod?} {endPeriod?} {startDate}';

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
        $startPeriod = $this->argument('startPeriod');
        $endPeriod = $this->argument('endPeriod');
        $startDate = $this->option('startDate');
        // Inisialisasi Controller
        $controller = new PartController();
        
        if ($endPeriod) {
            if (!$startPeriod) {
                $this->error('startPeriod harus diisi jika endPeriod diisi.');
                return Command::FAILURE;
            }

            try {
                // Buat objek Carbon dari string periode
                $start = Carbon::createFromFormat('ym', $startPeriod)->startOfMonth();
                $end = Carbon::createFromFormat('ym', $endPeriod)->startOfMonth();
            } catch (\Exception $e) {
                $this->error('Format periode tidak valid. Gunakan format "ym" (contoh: 2410).');
                Log::error('Invalid period format', ['start' => $startPeriod, 'end' => $endPeriod, 'error' => $e->getMessage()]);
                return Command::FAILURE;
            }

            if ($start > $end) {
                $this->error('startPeriod tidak boleh lebih besar dari endPeriod.');
                return Command::FAILURE;
            }

            $this->info("Memulai sinkronisasi data Part dari periode $startPeriod sampai $endPeriod...");
            if ($startDate) {
                $this->warn("Menggunakan filter StartDate: $startDate untuk SETIAP periode yang dijalankan.");
            }

            $current = $start->copy();
            $overallTotalProcessed = 0;
            $overallTotalBatches = 0;
            $successCount = 0;
            $failCount = 0;

            // Mulai Looping per bulan
            while ($current <= $end) {
                $periodString = $current->format('ym');
                $this->line('');
                $this->info("--- Memproses Periode: $periodString ---");

                // Panggil controller untuk periode ini
                $result = $controller->syncPartData($periodString, $startDate);

                if (!$result['success']) {
                    $errorMsg = $result['error'] ?? 'Unknown error';
                    $this->error("Sinkronisasi Gagal untuk $periodString! " . $errorMsg);
                    $failCount++;
                } else {
                    $this->info("Sinkronisasi Berhasil untuk $periodString!");
                    $this->table(
                        ['Metrik', 'Nilai'],
                        [
                            ['Filter StartDate', $result['filter_start_date']],
                            ['Filter Period', $result['filter_period']],
                            ['Total Baris Diproses', $result['total_processed_api_rows']],
                            ['Total Batch Database', $result['total_db_batches_processed']],
                        ]
                    );
                    // Akumulasi total
                    $overallTotalProcessed += $result['total_processed_api_rows'];
                    $overallTotalBatches += $result['total_db_batches_processed'];
                    $successCount++;
                }
                
                // Maju ke bulan berikutnya
                $current->addMonth();
            }

            // Tampilkan Summary Keseluruhan
            $this->line('');
            $this->info('--- Sinkronisasi Berhasil! ---');
            $this->table(
                ['Metrik Keseluruhan', 'Nilai'],
                [
                    ['Periode Sukses', $successCount],
                    ['Periode Gagal', $failCount],
                    ['Total Baris Diproses', $overallTotalProcessed],
                    ['Total Batch Database', $overallTotalBatches],
                ]
            );

            return $failCount > 0 ? Command::FAILURE : Command::SUCCESS;

        } 
        // Jika end period tidak diisi
        else {
            $this->info('Memulai sinkronisasi data Part dari Epicor');
            if ($startPeriod) {
                $this->info("Periode: $startPeriod");
            }
            if ($startDate) {
                $this->info("StartDate: $startDate");
            }

            $result = $controller->syncPartData($startPeriod, $startDate); 

            // Tampilkan hasil di konsol
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