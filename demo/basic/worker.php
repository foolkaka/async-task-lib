<?php
require_once __DIR__.'/../autoload.php';
use Asynclib\Core\Consumer;
use Asynclib\Exception\ExceptionInterface;

$consumer = new Consumer();
$consumer->setExchange('demo_basic');
$consumer->setQueue('demo_basic_queue');
$consumer->setRoutingKeys(['abc']);
$consumer->setSerialize(false);
$consumer->run(function($key, $msg){
    echo " [$key] $msg \n";
//    throw new Exception('test');
    sleep(10);
});
