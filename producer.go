package main

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var count int

func launchProducer(brokerList []string, config *sarama.Config, role string, duration time.Duration) {
	producer := newProducer(brokerList, config, role)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// send messages for 'duration'
	timer := time.NewTimer(duration)
	for {
		select {
		case <-timer.C:
			log.Printf("Timer expired\n")
			return
		default:
			sendMessage(producer, *topic)
			time.Sleep(1 * time.Second)
		}

	}
}

func newProducer(brokerList []string, config *sarama.Config, role string) sarama.SyncProducer {
	config.ClientID = role
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}
	return producer
}

func sendMessage(producer sarama.SyncProducer, topic string) {
	count++
	newMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("msg " + strconv.Itoa(count)),
	}
	partition, offset, err := producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	log.Printf("Message sent to %v/%d at offset %d\n", topic, partition, offset)
}
