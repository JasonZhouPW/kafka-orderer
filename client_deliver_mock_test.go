/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
