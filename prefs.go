package orderer

import "github.com/Shopify/sarama"

// PrefsImpl ...
type PrefsImpl struct {
	FlagsImpl
	ConcurrentReqs int
	PartitionID    int32
	RequiredAcks   sarama.RequiredAcks
	Version        sarama.KafkaVersion
}

// NewPrefs ...
func NewPrefs() *PrefsImpl {
	prefs := PrefsImpl{
		FlagsImpl:      FlagsImpl{},
		ConcurrentReqs: 1,
		PartitionID:    0,
		RequiredAcks:   sarama.WaitForAll,
		Version:        sarama.V0_9_0_1,
	}
	return &prefs
}

// FlagsImpl ...
type FlagsImpl struct {
	Brokers string
	Topic   string
	Port    int
	Verbose bool
}
