package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/kchristidis/kafka-orderer/ab"
)

type ordererImpl struct {
	userPrefs *prefs
	config    *cluster.Config
	producer  sarama.SyncProducer
	consumer  *cluster.Consumer
}

func newOrderer(userPrefs *prefs) *ordererImpl {
	x := new(ordererImpl)
	x.userPrefs = userPrefs
	x.config = x.newConfig()
	x.producer = x.newProducer()
	x.sendMessage([]byte("init")) // send a message to create the topic, otherwise the consumer will throw an exception
	x.consumer = x.newConsumer()
	return x
}

func (x *ordererImpl) newConfig() *cluster.Config {
	conf := cluster.NewConfig()
	conf.Net.MaxOpenRequests = x.userPrefs.concurrentReqs
	conf.Producer.RequiredAcks = x.userPrefs.requiredAcks
	conf.Version = x.userPrefs.version
	return conf
}

func (x *ordererImpl) newProducer() sarama.SyncProducer {
	brokers := strings.Split(x.userPrefs.brokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, &x.config.Config)
	if err != nil {
		panic(err)
	}
	return producer
}

func (x *ordererImpl) newConsumer() *cluster.Consumer {
	brokers := strings.Split(x.userPrefs.brokers, ",")
	topics := []string{x.userPrefs.topic}
	x.config.Consumer.Offsets.Initial = x.userPrefs.begin
	consumer, err := cluster.NewConsumer(brokers, x.userPrefs.group, topics, x.config)
	if err != nil {
		panic(err)
	}
	return consumer
}

func (x *ordererImpl) sendMessage(payload []byte) {
	newMessage := &sarama.ProducerMessage{
		Topic: x.userPrefs.topic,
		Value: sarama.ByteEncoder(payload),
	}
	partition, offset, err := x.producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	fmt.Fprintf(os.Stdout, "Message \"%s\" sent to %v/%d at offset %d\n", payload, x.userPrefs.topic, partition, offset)
}

func (x *ordererImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		x.sendMessage(msg.Data)
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
	}
}

func (x *ordererImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	return nil
}
