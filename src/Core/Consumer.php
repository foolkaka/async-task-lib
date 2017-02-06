<?php
namespace Asynclib\Core;

use Asynclib\Amq\Exchange;
use Asynclib\Amq\Queue;
use Asynclib\Amq\AmqFactory;
use PhpAmqpLib\Message\AMQPMessage;
class Consumer {

    use Queue, Exchange;

    private $serialize = true;

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

        /**
         * @param AMQPMessage $message
         */
        $callback = function($message) use ($process) {
            $routing_key = $message->delivery_info['routing_key'];
            $raw_data = $this->isSerialize() ? unserialize($message->getBody()) : json_decode($message->getBody(), 1);

            $result = call_user_func($process, $routing_key, $raw_data['body'], $raw_data['etime']);
            if ($result == false){
                $message->delivery_info['channel']->basic_nack($message->delivery_info['delivery_tag']);
            }else{
                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            }
        };
        $channel->basic_consume($this->getQueueName(), '', false, false, false, false, $callback);
        while(count($channel->callbacks)) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();
    }
}