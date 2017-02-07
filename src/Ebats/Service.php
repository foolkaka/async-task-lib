<?php
namespace Asynclib\Ebats;

use Asynclib\Core\Consumer;
use Asynclib\Core\Publish;
use Asynclib\Core\Logs;
class Service {

    const EXCHANGE_EVENT = 'ebats.core.event';
    const EXCHANGE_TASKS = 'ebats.core.tasks';
    const EXCHANGE_READY = 'ebats.task.ready';
    const EXCHANGE_DELAY = 'ebats.task.delay';
    const QUEUE_EVENT  = 'ebats.events';
    const QUEUE_TASKS  = 'ebats.tasks';
    const QUEUE_SCALER = 'ebats.scaler';


    public static function start(){
        Logs::info("Service init");
        //启动Worker
        $children = [];
        $methods = get_class_methods(self::class);
        foreach ($methods as $method){
            if ($method == 'start'){
                continue;
            }

            $process = new \swoole_process([self::class, $method]);
            $pid = $process->start();
            $children[] = $pid;
        }

        //回收子进程
        while($ret = \swoole_process::wait(true)) {
            //只要一个子进程挂掉就退出主进程,pm2会重新拉起主进程.
            foreach ($children as $pid){
                posix_kill($pid, SIGKILL);
            }
        }
    }

    public static function listenScaler(\swoole_process $swoole_process){
        Logs::info('Scaler start.');
        $consumer = new Consumer();
        $consumer->setQueue(self::QUEUE_SCALER);
        $consumer->run(function($key, $data){
            list($c, $jobid) = $data;
            $scaler = new Scaler($jobid);
            if ($c == 'incr'){
                $scaler->incr();
            }else{
                $scaler->clean();
            }
        });
    }

    public static function listenEvent() {
        $events = EventManager::getEvents();
        Logs::info('Loaded event '. json_encode($events).'.');
        Logs::info('Service-event start.');
        $consumer = new Consumer();
        $consumer->setExchange(self::EXCHANGE_EVENT);
        $consumer->setQueue(self::QUEUE_EVENT);
        $consumer->setRoutingKeys($events);
        $consumer->run(function($event, $msg){
            Logs::info("The event $event coming.");
            $tasks = EventManager::getTasks($event);
            /** @var Task $task */
            foreach ($tasks as $task){
                $task->setParams($msg);

                $publish = new Publish();
                $publish->setAutoClose(false);
                $publish->setExchange($task->getDelay() ? Service::EXCHANGE_DELAY : Service::EXCHANGE_READY);
                $publish->send($task, $task->getTopic(), $task->getDelay());
                Logs::info("[{$task->getTopic()}] {$task->getName()} published, delay: {$task->getDelay()}");
            }
        });
    }

    public static function listenTask() {
        Logs::info('Service-task start.');
        $consumer = new Consumer();
        $consumer->setExchange(self::EXCHANGE_TASKS);
        $consumer->setQueue(self::QUEUE_TASKS);
        $consumer->run(function($key, $task){
            /** @var Task $task */

            $publish = new Publish();
            $publish->setAutoClose(false);
            $publish->setExchange(Service::EXCHANGE_READY);
            $publish->send($task, $task->getTopic(), $task->getDelay());
            Logs::info("[{$task->getTopic()}] {$task->getName()} published, delay: {$task->getDelay()}");
        });
    }
}