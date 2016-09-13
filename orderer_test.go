package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
	"google.golang.org/grpc"
)

func mockNew(t *testing.T, config *ConfigImpl) Orderer {
	return &serverImpl{
		broadcaster: mockNewBroadcaster(t, config),
		deliverer:   mockNewDeliverer(t, config),
	}
}

type mockBroadcastServer struct {
	grpc.ServerStream
	incoming chan *ab.BroadcastMessage
	outgoing chan *ab.BroadcastReply
}

func newMockBroadcastServer() *mockBroadcastServer {
	return &mockBroadcastServer{
		incoming: make(chan *ab.BroadcastMessage),
		outgoing: make(chan *ab.BroadcastReply),
	}
}

// Recv ...
func (mbs *mockBroadcastServer) Recv() (*ab.BroadcastMessage, error) {
	return <-mbs.incoming, nil
}

// Send ...
func (mbs *mockBroadcastServer) Send(reply *ab.BroadcastReply) error {
	mbs.outgoing <- reply
	return nil
}

type mockDeliverServer struct {
	grpc.ServerStream
	incoming chan *ab.DeliverUpdate
	outgoing chan *ab.DeliverReply
}

func newMockDeliverServer() *mockDeliverServer {
	return &mockDeliverServer{
		incoming: make(chan *ab.DeliverUpdate),
		outgoing: make(chan *ab.DeliverReply),
	}
}

// Recv ...
func (mds *mockDeliverServer) Recv() (*ab.DeliverUpdate, error) {
	return <-mds.incoming, nil
}

// Send ...
func (mds *mockDeliverServer) Send(reply *ab.DeliverReply) error {
	mds.outgoing <- reply
	return nil
}
