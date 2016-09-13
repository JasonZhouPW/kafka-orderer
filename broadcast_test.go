package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

func mockNewBroadcaster(t *testing.T, config *ConfigImpl) Broadcaster {
	mb := &broadcasterImpl{
		producer:   mockNewProducer(t, config),
		batchChan:  make(chan *ab.BroadcastMessage, config.Batch.Size),
		messages:   []*ab.BroadcastMessage{&ab.BroadcastMessage{Data: []byte("genesis")}},
		nextNumber: 0,
	}
	return mb
}
