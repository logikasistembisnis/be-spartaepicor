<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Http\Controllers\DeleteRecordController;

class SyncDeleteRecord extends Command
{
    /**
     * Nama perintah yang dipanggil di Artisan
     * Contoh: php artisan sync:epicor-deleterec
     */
    protected $signature = 'sync:epicor-deleterec';

    /**
     * Deskripsi singkat command
     */
    protected $description = 'Mengambil data delete record dari Epicor API dan menghapus data di PostgreSQL sesuai SysRowID.';

    /**
     * Logika utama command
     */
    public function handle()
    {
        $this->info('Memulai sinkronisasi delete record dari Epicor...');

        $controller = new DeleteRecordController();
        $result = $controller->syncDeletedRecords();

        if (!$result['success']) {
            $errorMsg = $result['error'] ?? 'Unknown error';
            $this->error('Sinkronisasi Gagal! ' . $errorMsg);
            return Command::FAILURE;
        }

        $this->info('Sinkronisasi Delete Record Berhasil!');
        $this->table(
            ['Metrik', 'Nilai'],
            [
                ['Total Record Diproses', $result['total_processed']],
                ['Total Record Dihapus', $result['total_deleted']],
                ['Epoch Timestamp', $result['epoch_timestamp_used']],
            ]
        );

        return Command::SUCCESS;
    }
}
