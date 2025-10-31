<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;
use App\Console\Commands\SyncEpicorPartTran; 
use App\Console\Commands\SyncLaborDtl;
use App\Console\Commands\SyncRcvDtl;
use App\Console\Commands\SyncUD06;
use App\Console\Commands\SyncPart;
use App\Console\Commands\SyncWarehouse;
use App\Console\Commands\SyncUD11;

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
        SyncUD06::class,
        SyncPart::class,
        SyncWarehouse::class,
        SyncUD11::class,
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
