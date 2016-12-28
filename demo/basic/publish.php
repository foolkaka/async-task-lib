<?php
require_once __DIR__.'/../autoload.php';
use Asynclib\Core\Publish;

try{
    $publish = new Publish();
    $publish->setExchange('demo_basic');
    $publish->setSerialize(false);
    $publish->send('this is a basic message', 'abc');
}catch (Exception $exc){
    echo $exc->getMessage();
}