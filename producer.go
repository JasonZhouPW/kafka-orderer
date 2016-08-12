package main

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var count int

func launchProducer(brokerList []string, config *sarama.Config, initFlags flags) {
	producer := newProducer(brokerList, config, initFlags.role)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// send messages for 'duration'
	total := time.NewTimer(initFlags.duration)
	every := time.NewTicker(initFlags.period)
	for {
		select {
		case <-total.C:
			log.Printf("Timer expired\n")
			every.Stop()
			return
		case <-every.C:
			sendMessage(producer, initFlags.topic)
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
