<?php
namespace Asynclib\Exception;

/**
 * 外部服务异常类,抛出此一次可使消费者nack消息
 * ServiceException
 * @package Asynclib\Exception
 */
class ServiceException extends \RuntimeException implements ExceptionInterface{

}