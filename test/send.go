package main

import (
	"log"
	"msgevent/core"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	url := core.AmqpURL("../config/config.yml");

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("connection.open: %s", err)
	}

	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %s", err)
	}

	log.Printf("%s", time.Now())

	for i := 0; i < 1; i++ {
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         []byte("{\"hello\": \"world\"}"),
		}

		err = c.Publish("test_event_center1", "test_event_center1", false, false, msg)
		if err != nil {
			log.Fatalf("basic.publish: %v", err)
		}
	}
	log.Printf("%s", time.Now())
}
