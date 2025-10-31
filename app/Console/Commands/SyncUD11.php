<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\UD11Controller;
use Illuminate\Support\Facades\Log;

class SyncUD11 extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-ud11';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data UD11 dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        $this->info('Memulai sinkronisasi data UD11 dari Epicor');

        // 1. Inisialisasi Controller
        $controller = new UD11Controller();
        
        // 2. Panggil metode sinkronisasi data
        $result = $controller->syncUD11Data();

        // 3. Tampilkan hasil di konsol
        if (!$result['success']) {
            $errorMsg = $result['error'] ?? 'Unknown error';
            $this->error('Sinkronisasi Gagal! ' . $errorMsg);
            return Command::FAILURE;
        }

        $this->info('Sinkronisasi Berhasil!');
        
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
