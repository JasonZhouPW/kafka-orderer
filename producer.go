package orderer

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// Producer ...
type Producer interface {
	Send(payload []byte) error
	Close() error
}

type producerImpl struct {
	producer sarama.SyncProducer
	topic    string
}

func newProducer(config *ConfigImpl) Producer {
	brokerConfig := newBrokerConfig(config)
	p, err := sarama.NewSyncProducer(config.Brokers, brokerConfig)
	if err != nil {
		panic(fmt.Errorf("Failed to create Kafka producer: %v", err))
	}
	return &producerImpl{producer: p, topic: config.Topic}
}

func (p *producerImpl) Close() error {
	return p.producer.Close()
}

func (p *producerImpl) Send(payload []byte) error {
	_, offset, err := p.producer.SendMessage(newMsg(payload, p.topic))
	if err == nil {
		Logger.Debugf("Forwarded block %v to ordering service\n", offset)
	}
	return err
}
