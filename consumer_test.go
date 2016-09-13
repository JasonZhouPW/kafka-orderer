package orderer

import (
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

type mockConsumerImpl struct {
	count     int
	offset    int64
	parent    *mocks.Consumer
	partition *mocks.PartitionConsumer
	topic     string
}

func mockNewConsumer(t *testing.T, config *ConfigImpl, beginFrom int64) (Consumer, error) {
	parent := mocks.NewConsumer(t, nil)
	partition := parent.ExpectConsumePartition(config.Topic, config.PartitionID, beginFrom-1)
	partition.ExpectMessagesDrainedOnClose()
	mc := &mockConsumerImpl{
		offset:    (beginFrom - 1),
		parent:    parent,
		partition: partition,
		topic:     config.Topic,
	}
	return mc, nil
}

func (mc *mockConsumerImpl) Recv() <-chan *sarama.ConsumerMessage {
	mc.partition.YieldMessage(newConsumerMessage([]byte(strconv.Itoa(mc.count)), mc.topic))
	mc.count++
	return mc.partition.Messages()
}

// Close ...
func (mc *mockConsumerImpl) Close() error {
	if err := mc.partition.Close(); err != nil {
		return err
	}
	return mc.parent.Close()
}

func newConsumerMessage(payload []byte, topic string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Value: []byte("test"),
		Topic: topic,
	}
}
