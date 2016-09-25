package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

type mockClientDelivererImpl struct {
	clientDelivererImpl
	t *testing.T
}

func mockNewClientDeliverer(t *testing.T, config *ConfigImpl, deadChan chan struct{}) Deliverer {
	mockBrokerFunc := func(config *ConfigImpl) Broker {
		return mockNewBroker(t, config)
	}
	mockConsumerFunc := func(config *ConfigImpl, seek int64) (Consumer, error) {
		return mockNewConsumer(t, config, seek)
	}

	return &mockClientDelivererImpl{
		clientDelivererImpl: clientDelivererImpl{
			brokerFunc:   mockBrokerFunc,
			consumerFunc: mockConsumerFunc,

			config:   config,
			deadChan: deadChan,
			errChan:  make(chan error),
			updChan:  make(chan *ab.DeliverUpdate),
		},
		t: t,
	}
}
