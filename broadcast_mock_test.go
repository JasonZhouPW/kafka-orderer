package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

func mockNewBroadcaster(t *testing.T, config *ConfigImpl, seek int64, disk chan []byte) Broadcaster {
	mb := &broadcasterImpl{
		producer:   mockNewProducer(t, config, seek, disk),
		config:     config,
		batchChan:  make(chan *ab.BroadcastMessage, config.Batch.Size),
		messages:   []*ab.BroadcastMessage{&ab.BroadcastMessage{Data: []byte("checkpoint")}},
		nextNumber: uint64(seek),
	}
	return mb
}
