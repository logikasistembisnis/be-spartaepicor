<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;
use App\Console\Commands\SyncEpicorPartTran; 
use App\Console\Commands\SyncLaborDtl;
use App\Console\Commands\SyncRcvDtl;

class ConsoleServiceProvider extends ServiceProvider
{
    /**
     * Daftar Command yang harus dimuat oleh Lumen.
     *
     * @var array
     */
    protected $commands = [
        SyncEpicorPartTran::class,
        SyncLaborDtl::class,
        SyncRcvDtl::class,
    ];

    /**
     * Panggil semua command Artisan.
     *
     * @return void
     */
    public function register()
    {
        // Baris ini akan mendaftarkan semua kelas yang ada di properti $commands
        $this->commands($this->commands);
    }
}
