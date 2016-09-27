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
	"github.com/kchristidis/kafka-orderer/config"
	"google.golang.org/grpc"
)

func mockNew(t *testing.T, conf *config.TopLevel, disk chan []byte) Orderer {
	return &serverImpl{
		broadcaster: mockNewBroadcaster(t, conf, oldestOffset, disk),
		deliverer:   mockNewDeliverer(t, conf),
	}
}

type mockBroadcastStream struct {
	grpc.ServerStream
	incoming chan *ab.BroadcastMessage
	outgoing chan *ab.BroadcastReply
	t        *testing.T
}

func newMockBroadcastStream(t *testing.T) *mockBroadcastStream {
	return &mockBroadcastStream{
		incoming: make(chan *ab.BroadcastMessage),
		outgoing: make(chan *ab.BroadcastReply),
		t:        t,
	}
}

func (mbs *mockBroadcastStream) Recv() (*ab.BroadcastMessage, error) {
	return <-mbs.incoming, nil
}

func (mbs *mockBroadcastStream) Send(reply *ab.BroadcastReply) error {
	mbs.outgoing <- reply
	return nil
}

type mockDeliverStream struct {
	grpc.ServerStream
	incoming chan *ab.DeliverUpdate
	outgoing chan *ab.DeliverReply
	t        *testing.T
}

func newMockDeliverStream(t *testing.T) *mockDeliverStream {
	return &mockDeliverStream{
		incoming: make(chan *ab.DeliverUpdate),
		outgoing: make(chan *ab.DeliverReply),
		t:        t,
	}
}

func (mds *mockDeliverStream) Recv() (*ab.DeliverUpdate, error) {
	return <-mds.incoming, nil

}

func (mds *mockDeliverStream) Send(reply *ab.DeliverReply) error {
	mds.outgoing <- reply
	return nil
}
