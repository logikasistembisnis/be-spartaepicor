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

$router->get('/fetchdeletedrecords', 'DeleteRecordController@fetchDeletedRecords');

$router->get('/fetchdataparttran', 'PartTranController@fetchDataPartTran');
$router->get('/fetchdatalabordtl', 'LaborDtlController@fetchDataLaborDtl');
$router->get('/fetchdatarcvdtl', 'RcvDtlController@fetchDataRcvDtl');
$router->get('/fetchdataud06', 'UD06Controller@fetchDataUD06');
$router->get('/fetchdatapart', 'PartController@fetchDataPart');
$router->get('/fetchdatawarehouse', 'WarehouseController@fetchDataWarehouse');
$router->get('/fetchdataud11', 'UD11Controller@fetchDataUD11');
$router->get('/fetchdataopmaster', 'OpMasterController@fetchDataOpMaster');
$router->get('/fetchdatarcvhead', 'RcvHeadController@fetchDataRcvHead');
$router->get('/fetchdatawarehousebin', 'WarehouseBinController@fetchDataWarehouseBin');
$router->get('/fetchdatapoheader', 'PoHeaderController@fetchDataPoHeader');
$router->get('/fetchdatapodetail', 'PoDetailController@fetchDataPoDetail');
$router->get('/fetchdataporel', 'PoRelController@fetchDataPoRel');
$router->get('/fetchdatapartclass', 'PartClassController@fetchDataPartClass');
$router->get('/fetchdataresource', 'ResourceController@fetchDataResource');
$router->get('/fetchdatajobhead', 'JobHeadController@fetchDataJobHead');
$router->get('/fetchdataud101', 'UD101Controller@fetchDataUD101');
$router->get('/fetchdataud03', 'UD03Controller@fetchDataUD03');
$router->get('/fetchdatajobmtl', 'JobMtlController@fetchDataJobMtl');
$router->get('/fetchdataorderhed', 'OrderHedController@fetchDataOrderHed');
$router->get('/fetchdatavendor', 'VendorController@fetchDataVendor');