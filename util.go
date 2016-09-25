package orderer

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/kchristidis/kafka-orderer/ab"
	"golang.org/x/crypto/sha3"
)

const (
	ackOutOfRangeError  = "ACK out of range"
	seekOutOfRangeError = "Seek out of range"
)

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

func newOffsetReq(config *ConfigImpl, seek int64) *sarama.OffsetRequest {
	req := &sarama.OffsetRequest{}
	// If seek == -1, ask for the for the offset assigned to next new message
	// If seek == -2, ask for the earliest available offset
	// The last parameter in the AddBlock call is needed for God-knows-why reasons.
	// From the Kafka folks themselves: "We agree that this API is slightly funky."
	// https://mail-archives.apache.org/mod_mbox/kafka-users/201411.mbox/%3Cc159383825e04129b77253ffd6c448aa@BY2PR02MB505.namprd02.prod.outlook.com%3E
	req.AddBlock(config.Topic, config.PartitionID, seek, 1)
	return req
}
