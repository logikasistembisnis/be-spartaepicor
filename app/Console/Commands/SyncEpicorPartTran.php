<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\PartTranController;

class SyncEpicorPartTran extends Command
{
    /**
     * Nama perintah (command) yang akan dipanggil oleh Artisan/Crontab.
     * Digunakan sebagai: php artisan sync:epicor-parttran
     *
     * @var string
     */
    protected $signature = 'sync:epicor-parttran';

    /**
     * Deskripsi singkat perintah ini.
     *
     * @var string
     */
    protected $description = 'Mengambil data PartTran baru dari Epicor API dan menyinkronkannya ke database lokal.';

    /**
     * Metode utama yang dijalankan saat command ini dipanggil.
     *
     * @return int
     */
    public function handle()
    {
        $this->info('Memulai sinkronisasi data PartTran dari Epicor...');

        // 1. Inisialisasi Controller
        $controller = new PartTranController();
        
        // 2. Panggil metode sinkronisasi data
        // Metode ini akan mengembalikan array hasil atau error.
        $result = $controller->syncPartTranData();

        // 3. Tampilkan hasil di konsol
        if (isset($result['error'])) {
            $this->error('Sinkronisasi Gagal! ' . $result['error']);
            // Mengembalikan status FAILURE (1)
            return Command::FAILURE;
        }

        $this->info('Sinkronisasi Berhasil!');
        $this->table(
            ['Metrik', 'Nilai'],
            [
                ['Total Baris Diproses', $result['total_inserted']],
                ['Last TranNum Diproses', $result['last_trannum_processed']],
                ['Total Batch Database', $result['total_batches_processed']],
            ]
        );

        // Mengembalikan status SUCCESS (0)
        return Command::SUCCESS;
    }
}
