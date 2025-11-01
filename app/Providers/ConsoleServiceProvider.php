<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;
use App\Console\Commands\SyncDeleteRecord; 
use App\Console\Commands\SyncEpicorPartTran; 
use App\Console\Commands\SyncLaborDtl;
use App\Console\Commands\SyncRcvDtl;
use App\Console\Commands\SyncUD06;
use App\Console\Commands\SyncPart;
use App\Console\Commands\SyncWarehouse;
use App\Console\Commands\SyncUD11;
use App\Console\Commands\SyncOpMaster;
use App\Console\Commands\SyncRcvHead;
use App\Console\Commands\SyncWarehouseBin;
use App\Console\Commands\SyncPoHeader;
use App\Console\Commands\SyncPoDetail;
use App\Console\Commands\SyncPoRel;
use App\Console\Commands\SyncPartClass;
use App\Console\Commands\SyncResource;

class ConsoleServiceProvider extends ServiceProvider
{
    /**
     * Daftar Command yang harus dimuat oleh Lumen.
     *
     * @var array
     */
    protected $commands = [
        SyncDeleteRecord::class,
        SyncEpicorPartTran::class,
        SyncLaborDtl::class,
        SyncRcvDtl::class,
        SyncUD06::class,
        SyncPart::class,
        SyncWarehouse::class,
        SyncUD11::class,
        SyncOpMaster::class,
        SyncRcvHead::class,
        SyncWarehouseBin::class,
        SyncPoHeader::class,
        SyncPoDetail::class,
        SyncPoRel::class,
        SyncPartClass::class,
        SyncResource::class,
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
