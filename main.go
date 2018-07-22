package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func prettyPrint(s string) {
	log.Printf("%s", s)
}

func getRabbit() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial("amqp://tz:tz!@localhost:5630/")
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}

func setQoS(ch *amqp.Channel) {
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
}

func declareQueue(ch *amqp.Channel, name string) amqp.Queue {
	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return q
}

func consume(ch *amqp.Channel, qName string) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		qName, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to consume a queue")
	return msgs
}

func main() {

	conn, ch := getRabbit()
	setQoS(ch)
	defer conn.Close()
	defer ch.Close()

	jobQ := declareQueue(ch, "job_queue")
	settingQ := declareQueue(ch, "setting_queue")

	msgs := consume(ch, jobQ.Name)
	msgs2 := consume(ch, settingQ.Name)

	forever := make(chan bool)

	go func() {
		for {
			select {
			case d := <-msgs:
				prettyPrint(string(d.Body))
				d.Ack(false)
			case d := <-msgs2:
				prettyPrint(string(d.Body))
				d.Ack(false)
			}
			log.Printf("Done")

		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
