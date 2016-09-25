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
