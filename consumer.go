package orderer

import (
	"github.com/Shopify/sarama"
)

// Consumer allows the caller to receive a stream of messages from a Kafka partition
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

// Recv returns a channel with messages received from a Kafka partition
func (c *consumerImpl) Recv() <-chan *sarama.ConsumerMessage {
	return c.partition.Messages()
}

// Close shuts down the partition consumer
func (c *consumerImpl) Close() error {
	if err := c.partition.Close(); err != nil {
		return err
	}
	return c.parent.Close()
}
