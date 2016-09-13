package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

type mockClientDelivererImpl struct {
	clientDelivererImpl
	t *testing.T
}

func mockNewClientDeliverer(t *testing.T, config *ConfigImpl, deadChan chan struct{}) ClientDeliverer {
	return &mockClientDelivererImpl{
		clientDelivererImpl: clientDelivererImpl{
			config:   config,
			deadChan: deadChan,
			errChan:  make(chan error),
			updChan:  make(chan *ab.DeliverUpdate, 100),
		},
		t: t,
	}
}

// ResetConsumer ...
func (mcd *mockClientDelivererImpl) ResetConsumer(seek int64) (Consumer, error) {
	return mockNewConsumer(mcd.t, mcd.config, seek)
}
