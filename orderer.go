package orderer

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// ServerImpl ...
type ServerImpl struct {
	Prefs  *PrefsImpl
	Config *sarama.Config

	Producer sarama.SyncProducer
	Consumer *ConsumerImpl

	TokenChan chan struct{}
	lastACK   int64
}

// ConsumerImpl ...
type ConsumerImpl struct {
	parent    sarama.Consumer
	Partition sarama.PartitionConsumer
}

func newConfig(prefs *PrefsImpl) *sarama.Config {
	conf := sarama.NewConfig()
	// conf.Net.MaxOpenRequests = prefs.ConcurrentReqs
	// conf.Producer.RequiredAcks = prefs.RequiredAcks
	conf.Version = prefs.Version
	return conf
}

func newProducer(prefs *PrefsImpl, config *sarama.Config) sarama.SyncProducer {
	brokers := strings.Split(prefs.Brokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	return producer
}

func newConsumer(prefs *PrefsImpl, config *sarama.Config, beginFrom int64) *ConsumerImpl {
	brokers := strings.Split(prefs.Brokers, ",")
	parent, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	partition, err := parent.ConsumePartition(prefs.Topic, prefs.PartitionID, beginFrom)
	if err != nil {
		panic(err)
	}

	return &ConsumerImpl{parent: parent, Partition: partition}
}

// NewServer ...
func NewServer(prefs *PrefsImpl) *ServerImpl {
	s := ServerImpl{
		Prefs:  prefs,
		Config: newConfig(prefs),
	}
	s.Producer = newProducer(s.Prefs, s.Config)
	s.SendMessage([]byte("init")) // send a message to create the topic, otherwise the consumer will throw an exception
	return &s
}

// CloseProducer ...
func (s *ServerImpl) CloseProducer() {
	if s.Producer != nil {
		if err := s.Producer.Close(); err != nil {
			panic(err)
		}
	}
}

// CloseConsumer ...
func (s *ServerImpl) CloseConsumer() {
	if s.Consumer != nil {
		s.Consumer.Close()
	}
}

// Close ...
func (c *ConsumerImpl) Close() {
	if err := c.Partition.Close(); err != nil {
		panic(err)
	}
	if err := c.parent.Close(); err != nil {
		panic(err)
	}
}

// SendMessage ...
func (s *ServerImpl) SendMessage(payload []byte) error {
	newMessage := &sarama.ProducerMessage{
		Topic: s.Prefs.Topic,
		Value: sarama.ByteEncoder(payload),
	}
	partition, offset, err := s.Producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
		return err
	}
	fmt.Fprintf(os.Stdout, "Message \"%s\" sent to %v/%d at offset %d\n", payload, s.Prefs.Topic, partition, offset)
	return err
}

func disablePush(c chan struct{}) (chan struct{}, int64) {
	remaining := int64(len(c))
	// log.Printf("Token channel had %d elements remaining\n", remaining)
	c = nil
	return c, remaining
}

func enablePush(c chan struct{}, newTokenCount int64) chan struct{} {
	c = make(chan struct{}, newTokenCount)
	for i := int64(0); i < newTokenCount; i++ {
		c <- struct{}{}
	}
	log.Printf("Token channel now has %d elements\n", len(c))
	return c
}
