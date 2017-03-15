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
        $methods = get_class_methods(__CLASS__);
        foreach ($methods as $method){
            if (!stripos($method, 'Scheduler')){
                continue;
            }

            $children = array_merge($children, self::processCreate($method));
        }

        //回收子进程
        while($ret = \swoole_process::wait(true)) {
            //只要一个子进程挂掉就退出主进程,pm2会重新拉起主进程.
            foreach ($children as $pid){
                posix_kill($pid, SIGKILL);
            }
        }
    }

    public static function scalerScheduler(\swoole_process $swoole_process){
        Logs::info('scalerScheduler start.');
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

    public static function eventScheduler() {
        $events = EventManager::getEvents();
        Logs::info('Loaded event '. json_encode($events).'.');
        Logs::info('eventScheduler start.');
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

    public static function taskScheduler() {
        Logs::info('taskScheduler start.');
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

    private static function processCreate($scheduler){
        $children = [];
        for ($i = 0; $i < self::getProcessNum($scheduler); $i++){
            $process = new \swoole_process([__CLASS__, $scheduler]);
            $pid = $process->start();
            $children[] = $pid;
        }
        return $children;
    }

    private static function getProcessNum($scheduler_name){
        switch ($scheduler_name){
            case 'eventScheduler':
                return defined('EBATS_EVENT_NUM') ? EBATS_EVENT_NUM : 1;
            case 'taskScheduler':
                return defined('EBATS_TASKS_NUM') ? EBATS_TASKS_NUM : 1;
            default:
                return 1;
        }
    }
}