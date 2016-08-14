package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	cluster "github.com/bsm/sarama-cluster"
)

func launchBoth(config *cluster.Config, userPrefs *prefs) {
	producer := newProducer(config, userPrefs)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// send a message to create the topic, otherwise the consumer will throw an exception
	sendMessage(producer, userPrefs.topic)

	consumer := newConsumer(config, userPrefs)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	total := time.NewTimer(userPrefs.duration)
	every := time.NewTicker(userPrefs.period)

	for {
		select {
		case <-signals:
			return
		case <-total.C:
			log.Printf("Timer expired\n")
			every.Stop()
			return
		case <-every.C:
			sendMessage(producer, userPrefs.topic)
		case err := <-consumer.Errors():
			log.Printf("Error: %s\n", err.Error())
		case note := <-consumer.Notifications():
			log.Printf("Rebalanced: %+v\n", note)
		case message := <-consumer.Messages():
			fmt.Fprintf(os.Stdout, "Consumed message (topic: %s, part: %d, offset: %d, value: %s)\n",
				message.Topic, message.Partition, message.Offset, message.Value)
			consumer.MarkOffset(message, "")
		}
	}
}
