<?php
namespace Asynclib\Ebats;

use Asynclib\Amq\ExchangeTypes;
use Asynclib\Core\Consumer;
use Asynclib\Core\Publish;
use Asynclib\Core\Logs;
class Scheduler {

    const EXCHANGE_EVENT = 'ebats.core.event';
    const EXCHANGE_TASKS = 'ebats.core.tasks';
    const EXCHANGE_READY = 'ebats.task.ready';
    const EXCHANGE_DELAY = 'ebats.task.delay';
    const QUEUE_EVENT = 'ebats.events';
    const QUEUE_TASKS = 'ebats.tasks';

    public static function listenEvent() {
        $events = EventManager::getEvents();
        Logs::info('Loaded event '. json_encode($events).'.');
        Logs::info('Scheduler start.');
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
                $publish->setExchange($task->getDelay() ? Scheduler::EXCHANGE_DELAY : Scheduler::EXCHANGE_READY);
                $publish->send($task, $task->getTopic(), $task->getDelay());
                Logs::info("[{$task->getTopic()}] {$task->getName()} published, delay: {$task->getDelay()}");
            }
        });
    }

    public static function listenTask() {
        $consumer = new Consumer();
        $consumer->setExchange(self::EXCHANGE_TASKS);
        $consumer->setQueue(self::QUEUE_TASKS);
        $consumer->run(function($key, $task){
            /** @var Task $task */

            $publish = new Publish();
            $publish->setAutoClose(false);
            $publish->setExchange(Scheduler::EXCHANGE_READY);
            $publish->send($task, $task->getTopic(), $task->getDelay());
            Logs::info("[{$task->getTopic()}] {$task->getName()} published, delay: {$task->getDelay()}");
        });
    }
}