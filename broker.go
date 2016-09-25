package orderer

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// Broker allows the caller to get info on the orderer's stream
type Broker interface {
	GetOffset(req *sarama.OffsetRequest) (int64, error)
	Closeable
}

type brokerImpl struct {
	broker *sarama.Broker
	config *ConfigImpl
}

func newBroker(config *ConfigImpl) Broker {
	broker := sarama.NewBroker(config.Brokers[0])
	if err := broker.Open(nil); err != nil {
		panic(fmt.Errorf("Failed to create Kafka broker: %v", err))
	}
	return &brokerImpl{
		broker: broker,
		config: config,
	}
}

// GetOffset retrieves the offset number that corresponds to the requested position in the log
func (b *brokerImpl) GetOffset(req *sarama.OffsetRequest) (int64, error) {
	resp, err := b.broker.GetAvailableOffsets(req)
	if err != nil {
		return int64(-1), err
	}
	return resp.GetBlock(b.config.Topic, b.config.PartitionID).Offsets[0], nil
}

// Close terminates the broker
func (b *brokerImpl) Close() error {
	return b.broker.Close()
}
