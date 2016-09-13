package orderer

import (
	"sync"

	"github.com/kchristidis/kafka-orderer/ab"
)

// Deliverer ...
type Deliverer interface {
	Deliver(stream ab.AtomicBroadcast_DeliverServer) error
	Close() error
}

type delivererImpl struct {
	config   *ConfigImpl
	deadChan chan struct{}
	wg       sync.WaitGroup
}

func newDeliverer(config *ConfigImpl) Deliverer {
	return &delivererImpl{
		config:   config,
		deadChan: make(chan struct{}),
	}
}

// Deliver receives updates from connected clients and adjusts
// the transmission of ordered messages to them accordingly.
func (d *delivererImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	cd := newClientDeliverer(d.config, d.deadChan)

	d.wg.Add(1)
	defer d.wg.Done()

	defer cd.Close()
	return cd.Deliver(stream)
}

// Close ...
func (d *delivererImpl) Close() error {
	close(d.deadChan)
	d.wg.Wait() // Wait till all the client-deliverer consumers have closed
	return nil
}
