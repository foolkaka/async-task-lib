<?php
namespace Asynclib\Exception;

class RetryException extends \RuntimeException implements ExceptionInterface{

    private $retry;
    private $interval;

    public function __construct($message, $retry = null, $interval = null) {
        parent::__construct($message);
        $this->retry = $retry;
        $this->interval = $interval;
    }

    public function getRetry() {
        return $this->retry;
    }

    public function getInterval() {
        return $this->interval;
    }
}