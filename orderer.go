package orderer

import "github.com/kchristidis/kafka-orderer/ab"

// Orderer ...
type Orderer interface {
	Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error
	Deliver(stream ab.AtomicBroadcast_DeliverServer) error
	Teardown() error
}

type serverImpl struct {
	broadcaster Broadcaster
	deliverer   Deliverer
}

// New ...
func New(config *ConfigImpl) Orderer {
	return &serverImpl{
		broadcaster: newBroadcaster(config),
		deliverer:   newDeliverer(config),
	}
}

// Broadcast ...
func (s *serverImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	return s.broadcaster.Broadcast(stream)
}

// Deliver ...
func (s *serverImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	return s.deliverer.Deliver(stream)
}

// Teardown ...
func (s *serverImpl) Teardown() error {
	s.deliverer.Close()
	return s.broadcaster.Close()
}
