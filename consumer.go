package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func launchConsumer(brokerList []string, config *sarama.Config, role string, offset int64) {
	consumer := newConsumer(brokerList, config, role)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	prtCons, err := consumer.ConsumePartition(*topic, 0, offset) // for now, consume always consume partition 0
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := prtCons.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-signals:
			return
		case message := <-prtCons.Messages():
			log.Printf("Consumed message offset %d\n", message.Offset)
		}
	}
}

func newConsumer(brokerList []string, config *sarama.Config, role string) sarama.Consumer {
	config.ClientID = role
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	return consumer
}
