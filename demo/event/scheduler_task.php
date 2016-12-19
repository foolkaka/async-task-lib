<?php
require_once __DIR__.'/../autoload.php';
use Asynclib\Ebats\Scheduler;

//启动调度器
Scheduler::listenTask();

//Scheduler为系统常驻进程,建议使用pm2进行进程管理,防止异常情况下进程挂掉