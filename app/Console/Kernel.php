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
                ->dailyAt('01:00')
                ->sendOutputTo(storage_path('logs/scheduler-labordtl.log'))
                ->withoutOverlapping();
    }
}
