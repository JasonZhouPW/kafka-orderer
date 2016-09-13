package orderer

import (
	"github.com/Shopify/sarama"
)

// Consumer ...
type Consumer interface {
	Recv() <-chan *sarama.ConsumerMessage
	Close() error
}

type consumerImpl struct {
	parent    sarama.Consumer
	partition sarama.PartitionConsumer
}

func newConsumer(config *ConfigImpl, beginFrom int64) (Consumer, error) {
	parent, err := sarama.NewConsumer(config.Brokers, newBrokerConfig(config))
	if err != nil {
		return nil, err
	}
	partition, err := parent.ConsumePartition(config.Topic, config.PartitionID, beginFrom)
	if err != nil {
		return nil, err
	}
	c := &consumerImpl{parent: parent, partition: partition}
	Logger.Debug("Created new consumer for client beginning from block", beginFrom)
	return c, nil
}

// Recv ...
func (c *consumerImpl) Recv() <-chan *sarama.ConsumerMessage {
	return c.partition.Messages()
}

// Close ...
func (c *consumerImpl) Close() error {
	if err := c.partition.Close(); err != nil {
		return err
	}
	return c.parent.Close()
}
