<?php
namespace Asynclib\Ebats;

use Asynclib\Core\Publish;
class Event extends Publish{

    private $event;
    private $params;

    public function __construct($event_name) {
        parent::__construct();
        $this->event = $event_name;
    }

    public function setOptions($options = array()){
        $this->params = $options;
    }

    private function getEvent() {
        return $this->event;
    }

    private function getParams() {
        return $this->params;
    }

    public function publish() {
        $this->setAutoClose(false);
        $this->setExchange(Scheduler::EXCHANGE_EVENT);
        $this->send($this->getParams(), $this->getEvent());
    }
}