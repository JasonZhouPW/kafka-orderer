package orderer

import (
	"github.com/Shopify/sarama"
)

type consumerImpl struct {
	parent    sarama.Consumer
	partition sarama.PartitionConsumer
}

func (ds *deliverServerImpl) resetConsumer(seek, window int64) error {
	ds.disablePush()
	if err := ds.closeConsumer(); err != nil {
		return err
	}
	ds.lastACK = seek - 1
	Logger.Debug("Set last ACK for this client's consumer to", ds.lastACK)
	if err := ds.newConsumer(seek); err != nil {
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

func (ds *deliverServerImpl) newConsumer(beginFrom int64) error {
	parent, err := sarama.NewConsumer(ds.config.Brokers, ds.kafkaConfig)
	if err != nil {
		return err
	}

	partition, err := parent.ConsumePartition(ds.config.Topic, ds.config.PartitionID, beginFrom)
	if err != nil {
		return err
	}

	ds.consumer = &consumerImpl{parent: parent, partition: partition}
	Logger.Debug("Created new consumer for client beginning from block", beginFrom)
	return nil
}
