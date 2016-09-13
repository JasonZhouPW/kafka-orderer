package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

type mockDelivererImpl struct {
	delivererImpl
	t *testing.T
}

func mockNewDeliverer(t *testing.T, config *ConfigImpl) Deliverer {
	md := &mockDelivererImpl{
		delivererImpl: delivererImpl{
			config:   config,
			deadChan: make(chan struct{}),
		},
		t: t,
	}
	return md
}

// Deliver ...
func (md *mockDelivererImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	mcd := mockNewClientDeliverer(md.t, md.config, md.deadChan)

	md.wg.Add(1)
	defer md.wg.Done()

	defer mcd.Close()
	return mcd.Deliver(stream)
}
