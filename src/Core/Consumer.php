<?php
namespace Asynclib\Core;

use Asynclib\Amq\Exchange;
use Asynclib\Amq\Queue;
use Asynclib\Amq\AmqFactory;
use Asynclib\Exception\ServiceException;
use PhpAmqpLib\Channel\AbstractChannel;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
class Consumer {

    use Queue, Exchange;

    private $serialize = true;
    
    /** @var AMQPChannel */
    private $channel;

    public function setSerialize($serialize) {
        $this->serialize = $serialize;
    }

    private function isSerialize() {
        return $this->serialize;
    }

    public function run($process){
        $connection = AmqFactory::factory();
        $channel = $connection->channel();
        $channel->queue_declare($this->getQueueName(), false, true, false, false, false, $this->getArguments());
        if ($this->existsExchange()){
            $channel->exchange_declare($this->getExchangeName(), $this->getExchangeType(), false, true, false, false);
            foreach ($this->getRoutingKeys() as $routing_key){
                $channel->queue_bind($this->getQueueName(), $this->getExchangeName(), $routing_key);
            }
        }
        $this->channel = $channel;

        //增加断线重连机制,处理因网络异常导致的连接断开
        try{
            $this->consume($process);
        }catch (\Exception $exc){
            $connection->reconnect();
            $this->channel = $connection->channel();
            $this->consume($process);
        }
        $connection->close();
    }

    private function consume($process) {
        /**
         * @param AMQPMessage $message
         */
        $callback = function($message) use ($process) {
            $routing_key = $message->delivery_info['routing_key'];
            $raw_data = $this->isSerialize() ? unserialize($message->getBody()) : json_decode($message->getBody(), 1);

            try{
                call_user_func($process, $routing_key, $raw_data['body'], $raw_data['etime']);
                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            }catch (ServiceException $exc){
                usleep(100*1000);
                $message->delivery_info['channel']->basic_nack($message->delivery_info['delivery_tag'], false, true);
            }
        };

        $this->channel->basic_consume($this->getQueueName(), '', false, false, false, false, $callback);
        while(count($this->channel->callbacks)) {
            $this->channel->wait();
        }

        $this->channel->close();
    }
}