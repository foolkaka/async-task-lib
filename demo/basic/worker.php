<?php
require_once __DIR__.'/../autoload.php';
use Asynclib\Core\Consumer;
use Asynclib\Exception\ExceptionInterface;

$worker = new Consumer();
$worker->setExchange('demo_basic');
$worker->setQueue('demo_basic_queue');
$worker->setRoutingKeys(['abc']);
$worker->run(function($key, $msg){
    echo " [$key] $msg \n";
//    throw new Exception('test');
});
