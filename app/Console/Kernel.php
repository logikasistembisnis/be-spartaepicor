<?php

namespace App\Console;

use Illuminate\Console\Scheduling\Schedule;
use Laravel\Lumen\Console\Kernel as ConsoleKernel;

class Kernel extends ConsoleKernel
{
    /**
     * The Artisan commands provided by your application.
     *
     * @var array
     */
    protected $commands = [
        Commands\SyncDeleteRecord::class,
        Commands\SyncEpicorPartTran::class,
        Commands\SyncLaborDtl::class,
        Commands\SyncRcvDtl::class,
        Commands\SyncUD06::class,
        Commands\SyncPart::class,
        Commands\SyncWarehouse::class,
        Commands\SyncUD11::class,
        Commands\SyncOpMaster::class,
        Commands\SyncRcvHead::class,
        Commands\SyncWarehouseBin::class,
        Commands\SyncPoHeader::class,
        Commands\SyncPoRel::class,
        Commands\SyncPartClass::class,
        Commands\SyncResource::class,
        Commands\SyncJobHead::class,
        Commands\SyncUD101::class,
        Commands\SyncUD03::class,
        Commands\SyncJobMtl::class,
        Commands\SyncOrderHed::class,
        Commands\SyncVendor::class,
        Commands\SyncCustomer::class,
        Commands\SyncOrderDtl::class,
        Commands\SyncUD101A::class,
        Commands\SyncUD10::class,
    ];

    /**
     * Define the application's command schedule.
     *
     * @param  \Illuminate\Console\Scheduling\Schedule  $schedule
     * @return void
     */
    protected function schedule(Schedule $schedule)
    {
        $schedule->command('sync:epicor-parttran')
                ->hourly()
                ->sendOutputTo(storage_path('logs/scheduler-parttran.log'))
                ->withoutOverlapping(); 

        $schedule->command('sync:epicor-labordtl')
                ->hourly()
                ->sendOutputTo(storage_path('logs/scheduler-labordtl.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-rcvdtl')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-rcvdtl.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud06')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-ud06.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-part')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-part.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-warehouse')
                ->weeklyOn(1)
                ->sendOutputTo(storage_path('logs/scheduler-warehouse.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud11')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-ud11.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-opmaster')
                ->weeklyOn(1, '01:00')
                ->sendOutputTo(storage_path('logs/scheduler-opmaster.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-rcvhead')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-rcvhead.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-warehousebin')
                ->weeklyOn(1)
                ->sendOutputTo(storage_path('logs/scheduler-warehousebin.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-poheader')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-poheader.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-deleterec')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-deleterec.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-podetail')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-podetail.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-porel')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-porel.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-partclass')
                ->weeklyOn(1)
                ->sendOutputTo(storage_path('logs/scheduler-partclass.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-resource')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-resource.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-jobhead')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-jobhead.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud101')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-ud101.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud03')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-ud03.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-jobmtl')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-jobmtl.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-orderhed')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-orderhed.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-vendor')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-vendor.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-customer')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-customer.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-orderdtl')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-orderdtl.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud101a')
                ->cron('0 */2 * * *')
                ->sendOutputTo(storage_path('logs/scheduler-ud101a.log'))
                ->withoutOverlapping();
        $schedule->command('sync:epicor-ud10')
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-ud10.log'))
                ->withoutOverlapping();
    }
}