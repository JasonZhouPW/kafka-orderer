package orderer

import (
	"github.com/Shopify/sarama"
)

type consumerImpl struct {
	parent    sarama.Consumer
	partition sarama.PartitionConsumer
}

func (ds *deliverServerImpl) resetConsumer(config *ConfigImpl, seek, window int64) error {
	ds.disablePush()
	if err := ds.closeConsumer(); err != nil {
		return err
	}
	ds.lastACK = seek - 1
	Logger.Debug("Set last ACK for this client's consumer to", ds.lastACK)
	if err := ds.newConsumer(config, seek); err != nil {
		return err
	}
	ds.enablePush(window)
	return nil
}

func (ds *deliverServerImpl) closeConsumer() error {
	if ds.consumer != nil {
		return ds.consumer.close()
	}
	return nil
}

func (c *consumerImpl) close() error {
	if err := c.partition.Close(); err != nil {
		return err
	}
	if err := c.parent.Close(); err != nil {
		return err
	}
	return nil
}

func (ds *deliverServerImpl) newConsumer(config *ConfigImpl, beginFrom int64) error {
	brokerConfig := newBrokerConfig(config)
	parent, err := sarama.NewConsumer(config.Brokers, brokerConfig)
	if err != nil {
		return err
	}

	partition, err := parent.ConsumePartition(config.Topic, config.PartitionID, beginFrom)
	if err != nil {
		return err
	}

	ds.consumer = &consumerImpl{parent: parent, partition: partition}
	Logger.Debug("Created new consumer for client beginning from block", beginFrom)
	return nil
}
