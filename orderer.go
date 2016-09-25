package orderer

import "github.com/kchristidis/kafka-orderer/ab"

// Orderer allows the caller to submit to and receive messages from the orderer
type Orderer interface {
	Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error
	Deliver(stream ab.AtomicBroadcast_DeliverServer) error
	Teardown() error
}

// Closeable allows the shut down of the calling resource
type Closeable interface {
	Close() error
}

type serverImpl struct {
	broadcaster Broadcaster
	deliverer   Deliverer
}

// New creates a new orderer
func New(config *ConfigImpl) Orderer {
	return &serverImpl{
		broadcaster: newBroadcaster(config),
		deliverer:   newDeliverer(config),
	}
}

// Broadcast submits messages for ordering
func (s *serverImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	return s.broadcaster.Broadcast(stream)
}

// Deliver returns a stream of ordered messages
func (s *serverImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	return s.deliverer.Deliver(stream)
}

// Teardown shuts down the orderer
func (s *serverImpl) Teardown() error {
	s.deliverer.Close()
	return s.broadcaster.Close()
}
