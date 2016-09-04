package orderer

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

// ServerImpl ...
type ServerImpl struct {
	DyingChan, DeadChan chan struct{}
	Config              *ConfigImpl

	kafkaConfig *sarama.Config
	producer    sarama.SyncProducer
}

// NewServer ...
func NewServer(config *ConfigImpl) *ServerImpl {
	s := &ServerImpl{
		DyingChan:   make(chan struct{}),
		DeadChan:    make(chan struct{}),
		Config:      config,
		kafkaConfig: newKafkaConfig(config),
	}
	s.producer = newProducer(s.Config, s.kafkaConfig)
	// Send a message to create the topic, otherwise
	// the consumer will throw an exception.
	s.Send([]byte("init"))
	return s
}

func newKafkaConfig(config *ConfigImpl) *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	// kafkaConfig.Net.MaxOpenRequests = config.ConcurrentReqs
	// kafkaConfig.Producer.RequiredAcks = config.RequiredAcks
	kafkaConfig.Version = config.Version
	return kafkaConfig
}

func newProducer(config *ConfigImpl, kafkaConfig *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(config.Brokers, kafkaConfig)
	if err != nil {
		panic(fmt.Errorf("Failed to create Kafka producer: %v", err))
	}
	return producer
}

// Send ...
func (s *ServerImpl) Send(payload []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: s.Config.Topic,
		Value: sarama.ByteEncoder(payload),
	}
	partition, offset, err := s.producer.SendMessage(msg)
	if err == nil {
		log.Printf("Sent \"%s\" (partition: %v/%d, offset: %d)\n", payload, s.Config.Topic, partition, offset)
	}
	return err
}

// Close ...
func (s *ServerImpl) Close() error {
	if s.producer != nil {
		if err := s.producer.Close(); err != nil {
			return err
		}
	}
	return nil
}
