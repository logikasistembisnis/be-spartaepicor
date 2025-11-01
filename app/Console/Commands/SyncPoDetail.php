<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\PoDetailController;
use Illuminate\Support\Facades\Log;

class SyncPoDetail extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-podetail {period?} {startDate?}';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data PoDetail dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        $this->info('Memulai sinkronisasi data PoDetail dari Epicor');

        $period = $this->argument('period');
        $startDate = $this->argument('startDate');
        // Inisialisasi Controller
        $controller = new PoDetailController();
        
        // Panggil metode sinkronisasi data
        $result = $controller->syncPoDetailData($period, $startDate);

        // Tampilkan hasil di konsol
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
