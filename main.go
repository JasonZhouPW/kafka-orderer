package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	brokerAddr := "localhost:9092"
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, newConfig())
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("hello there!")}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}

func newConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.ClientID = "prod1"
	return conf
}
