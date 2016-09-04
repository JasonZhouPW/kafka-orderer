package orderer

import "github.com/Shopify/sarama"

// ConfigImpl ...
type ConfigImpl struct {
	FlagsImpl
	ConcurrentReqs int
	PartitionID    int32
	RequiredAcks   sarama.RequiredAcks
	Version        sarama.KafkaVersion
}

// FlagsImpl ...
type FlagsImpl struct {
	Brokers []string
	Topic   string
	Port    int
	Verbose bool
}

// NewConfig ...
func NewConfig() *ConfigImpl {
	return &ConfigImpl{
		FlagsImpl:      FlagsImpl{},
		ConcurrentReqs: 1,
		PartitionID:    0,
		RequiredAcks:   sarama.WaitForAll,
		Version:        sarama.V0_9_0_1,
	}
}
