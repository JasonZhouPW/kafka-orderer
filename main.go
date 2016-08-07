package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokerAddr  = "localhost:9092"
	prodID      = "prod1"
	topicName   = "test"
	messageText = "hello world"

	concurrentReqs = 1
	requiredAcks   = sarama.WaitForAll

	version = sarama.V0_9_0_1
)

func main() {
	producer := newProducer(brokerAddr, newConfig(concurrentReqs, requiredAcks, prodID, version))
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	for {
		sendMessage(producer, topicName, messageText)
		time.Sleep(1 * time.Second)
	}
}

func newConfig(concurrentReqs int, requiredAcks sarama.RequiredAcks, prodID string, version sarama.KafkaVersion) *sarama.Config {
	conf := sarama.NewConfig()
	conf.Net.MaxOpenRequests = concurrentReqs
	conf.Producer.RequiredAcks = requiredAcks
	conf.ClientID = prodID
	conf.Version = version
	return conf
}

func newProducer(brokerAddr string, config *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		log.Fatalln(err)
	}
	return producer
}

func sendMessage(producer sarama.SyncProducer, topic string, message string) {
	newMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
	partition, offset, err := producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
