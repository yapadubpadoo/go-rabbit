package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
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

func applySetting(x int, setting int) int {
	log.Printf("Apply %d with %d", x, setting)
	return x + setting
}

func applySettingViaAPI(x int, setting int) int {
	endpoint := "http://localhost:8080/process"
	requestURL := fmt.Sprintf("%s?number=%d&multiply-with=%d", endpoint, x, setting)
	log.Printf("[applySettingViaApi] Going to call %s", requestURL)
	r, _ := http.Get(requestURL)
	body, _ := ioutil.ReadAll(r.Body)
	log.Printf("[applySettingViaApi] result is %s", string(body))
	result, _ := strconv.Atoi(string(body))
	return result
}

func prettyPrint(s string) {
	log.Printf("[PrettyPrint] %s", s)
}

func main() {

	conn, ch := getRabbit()
	setQoS(ch)
	defer conn.Close()
	defer ch.Close()

	jobQ := declareQueue(ch, "job_queue")
	settingQ := declareQueue(ch, "setting_queue")

	jobMsgs := consume(ch, jobQ.Name)
	settingMsgs := consume(ch, settingQ.Name)

	forever := make(chan bool)
	currentSetting := 1

	go func(currentSetting *int) {
		for {
			select {
			case d := <-jobMsgs:
				x, _ := strconv.Atoi(string(d.Body))
				log.Printf("[Job] Incoming %d\n", x)
				// fmt.Println(string(applySetting(x, setting)))
				setting := *currentSetting
				result := applySettingViaAPI(x, setting)
				prettyPrint(strconv.Itoa(result))
				log.Printf("[Job] Result of x with setting is %d\n", result)
				d.Ack(false)
			case d := <-settingMsgs:
				log.Printf("[Setting] Current is %d", *currentSetting)
				newSetting, _ := strconv.Atoi(string(d.Body))
				*currentSetting = newSetting
				log.Printf("[Setting] Change to %d\n", *currentSetting)
				d.Ack(false)
				log.Printf("[Setting] Done")
			}
		}
	}(&currentSetting)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
