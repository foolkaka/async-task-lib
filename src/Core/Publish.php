<?php
namespace Asynclib\Core;

use Asynclib\Amq\AmqFactory;
use Asynclib\Amq\Exchange;
use Asynclib\Amq\Queue;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
class Publish {

    use Exchange, Queue;

    private $connection;
    private $auto_close = true;
    private $serialize = true;

    public function __construct() {
        $this->connection = AmqFactory::factory();
    }

    public function setAutoClose($auto_close) {
        $this->auto_close = $auto_close;
    }

    private function isAutoClose() {
        return $this->auto_close;
    }

    public function setSerialize($serialize) {
        $this->serialize = $serialize;
    }

    private function isSerialize() {
        return $this->serialize;
    }

    public function send($data, $routing_key = '', $delay = 0){
        $channel = $this->connection->channel();
        $channel->exchange_declare($this->getExchangeName(), $this->getExchangeType(), false, true, false);
        if ($this->getQueueName()){
            $channel->queue_declare($this->getQueueName(), false, true, false, false, false, $this->getArguments());
            foreach ($this->getRoutingKeys() as $routing_key){
                $channel->queue_bind($this->getQueueName(), $this->getExchangeName(), $routing_key);
            }
        }

        $raw_data = ['etime' => time() + $delay, 'body' => $data];
        $message = new AMQPMessage($this->isSerialize() ? serialize($raw_data) : json_encode($raw_data));
        $message->set('content_type', 'text/plain');
        $message->set('delivery_mode', AMQPMessage::DELIVERY_MODE_PERSISTENT);
        if ($delay){
            $message->set('expiration', $delay);
        }
        $channel->basic_publish($message, $this->getExchangeName(), $routing_key);
        $channel->close();
    }

    public function __destruct() {
        if ($this->isAutoClose()){
            $this->connection->close();
        }
    }
}