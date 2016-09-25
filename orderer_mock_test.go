package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
	"google.golang.org/grpc"
)

func mockNew(t *testing.T, config *ConfigImpl, disk chan []byte) Orderer {
	return &serverImpl{
		broadcaster: mockNewBroadcaster(t, config, oldestOffset, disk),
		deliverer:   mockNewDeliverer(t, config),
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
