package core

import (
	"github.com/streadway/amqp"
	"log"
	"msgevent/util"
)

// 初始化amqp
func AmqpInit() (*amqp.Connection, *amqp.Channel, error)  {
	url := AmqpURL("config/config.yml");

	conn, err := amqp.Dial(url)
	if  err != nil {
		util.LogOnError(err)
		return nil, nil, err
	}

	channel, err := conn.Channel()
	if  err != nil {
		util.LogOnError(err)
		return nil, nil, err
	}

	err = channel.Qos(1, 0, false)
	if  err != nil {
		util.LogOnError(err)
		return nil, nil, err
	}

	log.Printf("setup channel success!")

	return conn, channel, nil
}

// 注册交换机
func (qc QueueConfig) DeclareExchange (channel *amqp.Channel) {
	exchanges := []string{
		qc.WorkerExchangeName(),
		qc.RetryExchangeName(),
		qc.ErrorExchangeName(),
		qc.RequeueExchangeName(),
	}

	for _, e := range exchanges {
		log.Printf("declaring exchange: %s\n", e)

		err := channel.ExchangeDeclare(e, "topic", true, false, false, false ,nil)
		util.PanicOnError(err)
	}
}

// 注册队列
func (qc QueueConfig) DeclareQueue(channel *amqp.Channel) {
	var err error

	// 申明重试队列,并绑定到重试交换机
	log.Printf("declaring retry queue: %s\n", qc.RetryQueueName())
	retryQueueOptions := map[string]interface{}{
		"x-dead-letter-exchange": qc.RequeueExchangeName(),
		"x-message-ttl":          int32(qc.getRetryDuration() * 1000),
	}

	_, err = channel.QueueDeclare(qc.RetryQueueName(), true, false, false, false, retryQueueOptions)
	util.PanicOnError(err)
	err = channel.QueueBind(qc.RetryQueueName(), "#", qc.RetryExchangeName(), false, nil)
	util.PanicOnError(err)

	// 申明错误队列,并绑定到错误交换机
	log.Printf("declaring error queue: %s\n", qc.ErrorQueueName())

	_, err = channel.QueueDeclare(qc.ErrorQueueName(), true, false, false, false, nil)
	util.PanicOnError(err)
	err = channel.QueueBind(qc.ErrorQueueName(), "#", qc.ErrorExchangeName(), false, nil)

	// 申明工作队列,并绑定到工作交换机
	log.Printf("declaring worker queue: %s\n", qc.WorkerQueueName())
	workerQueueOptions := map[string]interface{}{
		"x-dead-letter-exchange": qc.RetryExchangeName(),
	}

	_,err = channel.QueueDeclare(qc.WorkerQueueName(), true, false, false, false, workerQueueOptions)
	util.PanicOnError(err)

	for _, key := range qc.RoutingKey {
		err = channel.QueueBind(qc.WorkerQueueName(), key, qc.WorkerExchangeName(), false, nil)
		util.PanicOnError(err)
	}

	// 工作队列绑定到重入队列交换机
	err = channel.QueueBind(qc.WorkerQueueName(), "#", qc.RequeueExchangeName(), false, nil)
	util.PanicOnError(err)
}

