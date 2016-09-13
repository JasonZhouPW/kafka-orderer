package orderer

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/kchristidis/kafka-orderer/ab"
	"golang.org/x/crypto/sha3"
)

func getOffset(config *ConfigImpl, beginFrom int64) (offset int64, err error) {
	broker := sarama.NewBroker(config.Brokers[0])
	err = broker.Open(nil)
	if err != nil {
		return
	}

	req := &sarama.OffsetRequest{}
	req.AddBlock(config.Topic, config.PartitionID, beginFrom, 1)
	resp, err := broker.GetAvailableOffsets(req)
	if err != nil {
		return
	}
	if err = broker.Close(); err != nil {
		return
	}

	return resp.GetBlock(config.Topic, config.PartitionID).Offsets[0], nil
}

func hashBlock(block *ab.Block) (hash, data []byte) {
	data, err := proto.Marshal(block)
	if err != nil {
		panic(fmt.Errorf("Failed to marshal block: %v", err))
	}

	hash = make([]byte, 64)
	sha3.ShakeSum256(hash, data)
	return
}

func newBrokerConfig(config *ConfigImpl) *sarama.Config {
	brokerConfig := sarama.NewConfig()
	// brokerConfig.Net.MaxOpenRequests = config.ConcurrentReqs
	// brokerConfig.Producer.RequiredAcks = config.RequiredAcks
	brokerConfig.Version = config.Version
	return brokerConfig
}

func newMsg(payload []byte, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	}
}
