<?php

/** @var \Laravel\Lumen\Routing\Router $router */

/*
|--------------------------------------------------------------------------
| Application Routes
|--------------------------------------------------------------------------
|
| Here is where you can register all of the routes for an application.
| It is a breeze. Simply tell Lumen the URIs it should respond to
| and give it the Closure to call when that URI is requested.
|
*/

$router->get('/', function () use ($router) {
    return $router->app->version();
});

$router->get('/fetchdataparttran', 'PartTranController@fetchDataPartTran');
$router->get('/fetchdatalabordtl', 'LaborDtlController@fetchDataLaborDtl');
$router->get('/fetchdatarcvdtl', 'RcvDtlController@fetchDataRcvDtl');
$router->get('/fetchdataud06', 'UD06Controller@fetchDataUD06');
$router->get('/fetchdatapart', 'PartController@fetchDataPart');
