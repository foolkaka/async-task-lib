<?php
namespace Asynclib\Ebats;

use Asynclib\Core\Consumer;
use Asynclib\Core\DeadLetter;
use Asynclib\Core\Logs;
use Asynclib\Core\Publish;
use Asynclib\Exception\ServiceException;
use Asynclib\Exception\RetryException;
use Asynclib\Exception\TaskException;
class Worker{

    private $callback;
    private $process_num;
    private $topic;
    private $delay = false;
    public static $retry_interval = [5, 300, 600, 3600, 7800];  //5s 5min 10min 1h 2h
    private $task_ready = 'ebats.task.%sR';
    private $task_delay = 'ebats.task.%sD';
    private $task_retry = 'ebats.task.d%s';

    const STATE_ERR   = -1;
    const STATE_SUCC  = 0;
    const STATE_FAIL  = 1;
    const STATE_RETRY = 2;

    public function __construct($callback, $process_num = 1) {
        $this->callback = $callback;
        $this->process_num = $process_num;
    }

    public function setTopic($topic, $delay = false) {
        $this->topic = $topic;
        $this->delay = $delay;
    }

    /**
     * @param string $key
     * @param Task $task
     * @return mixed
     */
    public function exec($topic, $task) {
        $topic = ucfirst($topic);
        $params = json_encode($task->getParams());
        Logs::info("[$topic]{$task->getName()} start exec with the params - $params.");
        $class = "Task{$topic}Model";
        $action = "{$task->getName()}Task";
        if (!class_exists($class)){
            Logs::error("[$topic] $class is not exists.");
            return -1;
        }

        $timebegin = microtime(true);//标记任务开始执行时间
        $model = new $class();
        if (!method_exists($model, $action)){
            Logs::error("[$topic] $action is not exists.");
            return -1;
        }
        $model->$action($task->getParams());
        $endtime = microtime(true); //标记任务执行完成时间
        $timeuse = ($endtime - $timebegin) * 1000; //计算任务执行用时
        Logs::info("[$topic]{$task->getName()} exec finished, timeuse - {$timeuse}ms.");
        return $timeuse;
    }

    /**
     * 重试机制
     * @param string $key
     * @param Task $task
     * @param int $retry 重试次数
     * @param int $interval 重试间隔
     */
    private function retry($key, $task, $retry, $interval){
        $scaler = new Scaler($task->getId());
        $fail_times = $scaler->get() + 1;
        $retry_times = $retry ? : count(self::$retry_interval);   //重试次数
        $delay_time = $retry ? $interval : self::$retry_interval[$fail_times - 1]; //重试间隔
        if ($fail_times > $retry_times){
            $scaler->cleanFanout();
            Logs::info("[$key]{$task->getName()} exec failed, retry end.");
            return $fail_times;
        }

        Logs::info("[$key]{$task->getName()} exec failed, after $delay_time seconds retry[$fail_times/$retry_times].");

        $publish = new Publish();
        $publish->setAutoClose(false);
        $publish->setExchange(Service::EXCHANGE_DELAY);
        $publish->setQueue(sprintf($this->task_retry, $delay_time));
        $publish->setRoutingKeys([$delay_time]);
        $publish->setLetter(Service::EXCHANGE_TASKS);
        $publish->send($task, $delay_time, $delay_time * 1000);

        $scaler->incrFanout();
        return $fail_times;
    }

    private function initDeadLetter(){
        $deadletter = new DeadLetter();
        $deadletter->setExchange(Service::EXCHANGE_DELAY);
        $deadletter->setQueue(sprintf($this->task_delay, $this->topic));
        $deadletter->setRoutingKeys([$this->topic]);
        $deadletter->setLetter(Service::EXCHANGE_READY, $this->topic);
        $deadletter->create();
    }

    public function process(\swoole_process $swoole_process){
        try{
            $this->initDeadLetter();
            $consumer = new Consumer();
            $consumer->setExchange(Service::EXCHANGE_READY);
            $consumer->setQueue(sprintf($this->task_ready, $this->topic));
            $consumer->setRoutingKeys([$this->topic]);
            $consumer->run(function($key, $task){
                /** @var Task $task */
                $timeuse = -1;
                $exectimes = 1;
                $status_code = self::STATE_SUCC;
                $status_msg = 'ok.';
                try{
                    $timeuse = $this->exec($key, $task);
                }catch (ServiceException $exc){
                    $status_code = self::STATE_ERR;
                    $status_msg = $exc->getMessage();
                    Logs::error("[$key]{$task->getName() } exec failed  - $status_msg");
                }catch (RetryException $exc){
                    $status_code = self::STATE_RETRY;
                    $status_msg = $exc->getMessage();
                    Logs::error("[$key]{$task->getName()} exec failed  - $status_msg");
                    $exectimes = $this->retry($key, $task, $exc->getRetry(), $exc->getInterval());
                }catch (TaskException $exc){
                    $status_code = self::STATE_FAIL;
                    $status_msg = $exc->getMessage();
                    Logs::error("[$key]{$task->getName()} exec failed  - $status_msg");
                }

                //将执行情况回调给上层开发者
                call_user_func($this->callback, $task, $status_code, $status_msg, $exectimes, $timeuse);
                //判断是否为ServiceException,如果是则进入重试
                if ($status_code == self::STATE_ERR && !empty($exc)){
                    $scaler = new Scaler($task->getId());
                    $fail_times = $scaler->get() + 1;
                    if ($fail_times < 20){
                        $scaler->incrFanout();
                        throw $exc;
                    }else{
                        $scaler->cleanFanout();
                    }
                }
            });

            $swoole_process->exit(0);
            exit;
        }catch (\Exception $exc){
            die($exc->getMessage()."\n");
        }
    }

    public function run(){
        Logs::info("Worker start init, process_num is {$this->process_num}.");
        //启动Worker
        for($i = 0; $i < $this->process_num; $i++){
            $process = new \swoole_process([$this, 'process']);
            $process->start();
        }
        Logs::info("Worker init finished.\n");
        while($ret = \swoole_process::wait(true)) {
            Logs::info("Worker exited - PID={$ret['pid']}\n");
        }
    }
}