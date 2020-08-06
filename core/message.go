package core

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"log"
	"msgevent/protocols"
	"msgevent/util"
	"net/http"
	"sync"
	"time"
)

type NotifyResponse int

const (
	NotifySuccess = 1
	NotifyFailure = 0

	ChannelBufferLength = 100 // chan 缓冲区大小
	ReceiverNum         = 2   // 消息接收者数量
	AckerNum            = 4  // 消息执行者数量
	ResenderNum         = 1   // 发送消息到错误队列的协程数量

	HttpMaxIdleConns        = 500 // default 100 in net/http
	HttpMaxIdleConnsPerHost = 500 // default 2 in net/http
	HttpIdleConnTimeout     = 30  // default 90 in net/http
)

// 消息体
type Message struct {
	queueConfig    QueueConfig
	amqpDelivery   *amqp.Delivery // message read from rabbitmq
	notifyResponse NotifyResponse // notify result from callback url
}

// 接收消息
func receiveMessage(queues []*QueueConfig, done <-chan struct{}) <-chan Message {
	out := make(chan Message, ChannelBufferLength)
	var wg sync.WaitGroup

	receiver := func(qc QueueConfig) {
		defer wg.Done()

		RECONNECT:
			for  {
				_, channel, err := AmqpInit()
				if err != nil {
					util.PanicOnError(err)
				}

				msgs, err := channel.Consume(
					qc.WorkerQueueName(),
					"",
					false,
					false,
					false,
					false,
					nil,
				)
				util.PanicOnError(err)

				for  {
					select {
						case msg, ok := <-msgs:
							if !ok {
								log.Printf("receiver: channel is closed, maybe lost connection")
								time.Sleep(5 * time.Second)
								continue RECONNECT
							}
							msg.MessageId = fmt.Sprintf("%s", uuid.NewV4())
							message := Message{qc, &msg, 0}
							out <- message

							message.Printf("receiver: received msg")
						case <-done:
							log.Printf("receiver: received a done signal")
							return
					}
				}
			}
	}

	for _, queue := range queues {
		wg.Add(ReceiverNum)
		for i := 0; i < ReceiverNum; i++ {
			go receiver(*queue)
		}
	}

	go func() {
		wg.Wait()
		log.Printf("all receiver is done, closing channel")
		close(out)
	}()

	return out
}

// 发送消息
func workMessage(in <-chan Message) <-chan Message {
	var wg sync.WaitGroup
	out := make(chan Message, ChannelBufferLength)
	client := protocols.HttpClient(HttpMaxIdleConns, HttpMaxIdleConnsPerHost, HttpIdleConnTimeout)

	worker := func(m Message, o chan<- Message) {
		m.Printf("worker: received a msg, body: %s", string(m.amqpDelivery.Body))

		defer wg.Done()
		m.Notify(client)
		o <- m
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for message := range in {
			wg.Add(1)
			go worker(message, out)
		}
	}()

	go func() {
		wg.Wait()
		log.Printf("all worker is done, closing channel")
		close(out)
	}()

	return out
}

// 确认消息
func ackMessage(in <-chan Message) <-chan Message {
	out := make(chan Message, ChannelBufferLength)
	var wg sync.WaitGroup

	acker := func() {
		defer wg.Done()

		for m := range in {
			m.Printf("acker: received a msg")
			if m.notifyResponse == NotifySuccess { // 消息处理成功
				m.Ack()
			} else if m.CurrentMessageRetries() >= m.queueConfig.getRetryNum() { // 重试次数大于最大重试次数
				m.Republish(out)
			} else { // 消息处理失败，进入重试交换机，并清除work交换机数据
				m.Reject()
			}
		}
	}

	for i := 0; i < AckerNum; i++ {
		wg.Add(1)
		go acker()
	}

	go func() {
		wg.Wait()
		log.Printf("all acker is done, close out")
		close(out)
	}()

	return out
}

// 发送消息
func SendMessage(queues []*QueueConfig, done <-chan struct{}) {
	<-resendMessage(ackMessage(workMessage(receiveMessage(queues, done))))
}

// 将重试队列的消息发送到错误队列
func resendMessage(in <-chan Message) <-chan Message {
	out := make(chan Message)
	var wg sync.WaitGroup

	resender := func() {
		defer wg.Done()

		RECONNECT:
			for {
				conn, channel, err := AmqpInit()
				if err != nil {
					util.PanicOnError(err)
				}

				for m := range in {
					err := m.CloneAndPublishToError(channel)
					if  err == amqp.ErrClosed {
						time.Sleep(5 * time.Second)
						continue RECONNECT
					}
				}

				conn.Close()
				break
			}
	}

	for i := 0; i < ResenderNum; i++ {
		wg.Add(1)
		go resender()
	}

	go func() {
		wg.Wait()
		log.Printf("all resender is done, close out")
		close(out)
	}()

	return out
}


// 输出消息日志
func (m Message) Printf(v ...interface{}) {
	msg := m.amqpDelivery

	vv := []interface{}{}
	vv = append(vv, msg.MessageId, msg.RoutingKey)
	vv = append(vv, v[1:]...)

	log.Printf("[%s] [%s] " + v[0].(string), vv...)
}

// 消息发送
func (m * Message) Notify(client *http.Client) * Message {
	qc := m.queueConfig
	msg := m.amqpDelivery

	client.Timeout = time.Duration(qc.NotifyTimeout()) * time.Second
	statusCode := protocols.NotifyUrl(client, qc.NotifyUrl(), msg.Body)

	m.Printf("notify url %s, result: %d", qc.NotifyUrl(), statusCode)

	if  statusCode == 200 {
		m.notifyResponse = NotifySuccess
	} else {
		m.notifyResponse = NotifyFailure
	}

	return m
}

// 消息确认
func (m Message) Ack() error {
	m.Printf("acker: ack message")
	err := m.amqpDelivery.Ack(false)
	util.LogOnError(err)
	return err
}

// 消息重试时间
func (m Message) CurrentMessageRetries() int {
	msg := m.amqpDelivery

	xDeathArray, ok := msg.Headers["x-death"].([]interface{})
	if !ok {
		m.Printf("x-death array case fail")
		return 0
	}

	if len(xDeathArray) <= 0 {
		return  0
	}

	for _, h := range xDeathArray {
		xDeathItem := h.(amqp.Table)
		if xDeathItem["reason"] == "rejected" {
			return int(xDeathItem["count"].(int64))
		}
	}

	return 0
}

// 消息重发
func (m Message) Republish(out chan<- Message) error {
	m.Printf("acker: ERROR republish message")
	out <- m
	err := m.amqpDelivery.Ack(false)
	util.LogOnError(err)
	return err
}

// 拒绝接收消息，并从队列中移除
func (m Message) Reject() error {
	m.Printf("acker: reject message")
	err := m.amqpDelivery.Reject(false)
	util.LogOnError(err)
	return err
}

// 复制重试队列发送过来的消息，并推送到错误队列
func (m Message) CloneAndPublishToError(channel *amqp.Channel) error {
	msg := m.amqpDelivery
	qc := m.queueConfig

	errMsg := cloneToPublishMsg(msg)
	err := channel.Publish(qc.ErrorExchangeName(), msg.RoutingKey, false, false, *errMsg)
	util.LogOnError(err)
	return err
}

// 复制消息
func cloneToPublishMsg(msg *amqp.Delivery) *amqp.Publishing {
	newMsg := amqp.Publishing{
		Headers: msg.Headers,

		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,

		Body: msg.Body,
	}

	return &newMsg
}