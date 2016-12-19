<?php
namespace Asynclib\Amq;

use PhpAmqpLib\Wire\AMQPTable;
trait Queue {

    private $name;
    private $expires;
    private $routing_keys = array('');
    private $letter_exchange;
    private $letter_routing_key;
    private $arguments = null;

    public function setLetter($letter_exchange, $letter_routing_key = '') {
        $this->letter_exchange = $letter_exchange;
        $this->letter_routing_key = $letter_routing_key;
    }

    public function setQueue($name, $expires = 0) {
        $this->name = $name;
        $this->expires = $expires;
    }

    public function setRoutingKeys($routing_keys) {
        $this->routing_keys = $routing_keys;
    }

    public function getQueueName() {
        return $this->name;
    }

    public function getRoutingKeys() {
        return $this->routing_keys;
    }

    public function getArguments() {
        $data = [];
        if ($this->letter_exchange){
            $data['x-dead-letter-exchange'] = $this->letter_exchange;
            $data['x-dead-letter-routing-key'] = $this->letter_routing_key;
        }
        if ($this->expires){
            $data['x-expires'] = $this->expires;
        }
        return new AMQPTable($data);
    }
}