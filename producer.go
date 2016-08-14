package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

var count int

func launchProducer(config *cluster.Config, userPrefs *prefs) {
	producer := newProducer(config, userPrefs)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// send messages for 'duration'
	total := time.NewTimer(userPrefs.duration)
	every := time.NewTicker(userPrefs.period)
	for {
		select {
		case <-total.C:
			log.Printf("Timer expired\n")
			every.Stop()
			return
		case <-every.C:
			sendMessage(producer, userPrefs.topic)
		}

	}
}

func newProducer(config *cluster.Config, userPrefs *prefs) sarama.SyncProducer {
	brokers := strings.Split(userPrefs.brokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
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
	fmt.Fprintf(os.Stdout, "Message sent to %v/%d at offset %d\n", topic, partition, offset)
}
