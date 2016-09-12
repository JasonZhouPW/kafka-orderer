package orderer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kchristidis/kafka-orderer/ab"
)

// Orderer ...
type Orderer interface {
	Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error
	Deliver(stream ab.AtomicBroadcast_DeliverServer) error
	Teardown() error
}

type serverImpl struct {
	config      *ConfigImpl
	broadcaster *broadcastServerImpl
	deadChan    chan struct{}
	wg          sync.WaitGroup
}

// New ...
func New(config *ConfigImpl) Orderer {
	s := &serverImpl{
		config:   config,
		deadChan: make(chan struct{}),
	}
	s.broadcaster = newBroadcastServer(s)
	return s
}

// Teardown ...
func (s *serverImpl) Teardown() error {
	close(s.deadChan)
	s.wg.Wait() // Wait till all the deliver consumers have closed
	if s.broadcaster.producer != nil {
		if err := s.broadcaster.producer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func newBrokerConfig(config *ConfigImpl) *sarama.Config {
	brokerConfig := sarama.NewConfig()
	// brokerConfig.Net.MaxOpenRequests = config.ConcurrentReqs
	// brokerConfig.Producer.RequiredAcks = config.RequiredAcks
	brokerConfig.Version = config.Version
	return brokerConfig
}
