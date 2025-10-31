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
        Commands\SyncEpicorPartTran::class,
        Commands\SyncLaborDtl::class
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
    }
}