package orderer

import (
	"sync"

	"github.com/Shopify/sarama"
)

// ServerImpl ...
type ServerImpl struct {
	Config      *ConfigImpl
	broadcaster *broadcastServerImpl
	deadChan    chan struct{}
	wg          sync.WaitGroup
}

// NewServer ...
func NewServer(config *ConfigImpl) *ServerImpl {
	s := &ServerImpl{
		Config:   config,
		deadChan: make(chan struct{}),
	}
	s.broadcaster = newBroadcastServer(s)
	return s
}

// Teardown ...
func (s *ServerImpl) Teardown() error {
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
