<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\CustomerController;
use Illuminate\Support\Facades\Log;

class SyncCustomer extends Command
{
    /**
     * Nama perintah (command)
     * @var string
     */
    protected $signature = 'sync:epicor-customer';

    /**
     * Deskripsi singkat perintah ini.
     * @var string
     */
    protected $description = 'Mengambil data Customer harian dari Epicor API dan menyinkronkannya (UPSERT).';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     * @return int
     */
    public function handle()
    {
        $this->info('Memulai sinkronisasi data Customer dari Epicor');

        // 1. Inisialisasi Controller
        $controller = new CustomerController();
        
        // 2. Panggil metode sinkronisasi data
        $result = $controller->syncCustomerData();

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
                ['Total Baris Diproses', $result['total_processed_api_rows']],
                ['Total Batch Database', $result['total_db_batches_processed']],
            ]
        );

        return Command::SUCCESS;
    }
}
