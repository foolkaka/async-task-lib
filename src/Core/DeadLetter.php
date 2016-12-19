<?php
namespace Asynclib\Core;

use Asynclib\Amq\Queue;
use Asynclib\Amq\Exchange;
use Asynclib\Amq\AmqFactory;
class DeadLetter{

    use Exchange, Queue;

    public function create(){
        $connection = AmqFactory::factory();
        $channel = $connection->channel();
        $channel->queue_declare($this->getQueueName(), false, true, false, false, false, $this->getArguments());
        $channel->exchange_declare($this->getExchangeName(), $this->getExchangeType(), false, true, false, false, false);
        foreach ($this->getRoutingKeys() as $routing_key){
            $channel->queue_bind($this->getQueueName(), $this->getExchangeName(), $routing_key);
        }
    }
}