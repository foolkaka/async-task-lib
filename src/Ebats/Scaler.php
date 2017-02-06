<?php
namespace Asynclib\Ebats;

/**
 * 分布式计数类
 */
use Asynclib\Core\Publish;
class Scaler {

    private $dir = '/tmp/ebats/';
    private $sid;
    private $filename;

    public function __construct($id) {
        $this->sid = $id;
        if (!file_exists($this->dir)){
            mkdir($this->dir);
        }
        $filename = $this->dir . $id;
        if (!file_exists($filename)){
            touch($filename);
        }

        $this->filename = $filename;
    }

    /**
     * 获取计数值
     */
    public function get(){
        $fp = fopen($this->filename, 'r+');
        $content = fgets($fp, 50);
        fclose($fp);
        return intval($content);
    }

    /**
     * 本地计数+1
     */
    public function incr(){
        $fp = fopen($this->filename, 'r+');
        $new_num = $this->get() + 1;
        fwrite($fp, $new_num);
        fclose($fp);
    }

    /**
     * 本地计数清除
     */
    public function clean(){
        unlink($this->filename);
    }

    /**
     * 广播计数+1
     */
    public function incrFanout(){
        $this->publish('incr');
    }

    /**
     * 广播计数清除
     */
    public function cleanFanout(){
        $this->publish('clean');
    }

    private function publish($cmd = 'incr'){
        $data = [$cmd, $this->sid];
        $publish = new Publish();
        $publish->setAutoClose(false);
        $publish->setQueue(Service::QUEUE_SCALER);
        $publish->send($data, Service::QUEUE_SCALER);
    }
}