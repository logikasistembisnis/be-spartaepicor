<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\LaborDtlController;
use Illuminate\Support\Facades\Log;

class SyncLaborDtl extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-labordtl';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data LaborDtl harian dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        Log::channel('cron')->info('=== MEMULAI CRON JOB: sync:epicor-labordtl ===');
        $this->info('Memulai sinkronisasi data LaborDtl dari Epicor');

        // 1. Inisialisasi Controller
        $controller = new LaborDtlController();
        
        // 2. Panggil metode sinkronisasi data
        $result = $controller->syncLaborDtlData();

        // 3. Tampilkan hasil di konsol
        if (!$result['success']) {
            $errorMsg = $result['error'] ?? 'Unknown error';
            $this->error('Sinkronisasi Gagal! ' . $errorMsg);
            Log::channel('cron')->error('HASIL: GAGAL.', $result);
            return Command::FAILURE;
        }

        $this->info('Sinkronisasi Berhasil!');
        Log::channel('cron')->info('HASIL: SUKSES.', $result);
        
        // Sesuaikan tabel output
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
