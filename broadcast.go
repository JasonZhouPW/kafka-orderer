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
	"sync"
	"time"

	"github.com/kchristidis/kafka-orderer/ab"
	"github.com/kchristidis/kafka-orderer/config"
)

// Broadcaster allows the caller to submit messages to the orderer
type Broadcaster interface {
	Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error
	Closeable
}

type broadcasterImpl struct {
	producer Producer
	config   *config.TopLevel
	once     sync.Once

	batchChan  chan *ab.BroadcastMessage
	errChan    chan error
	messages   []*ab.BroadcastMessage
	nextNumber uint64
	prevHash   []byte
}

func newBroadcaster(conf *config.TopLevel) Broadcaster {
	return &broadcasterImpl{
		producer:   newProducer(conf),
		config:     conf,
		batchChan:  make(chan *ab.BroadcastMessage, conf.General.BatchSize),
		errChan:    make(chan error),
		messages:   []*ab.BroadcastMessage{&ab.BroadcastMessage{Data: []byte("genesis")}},
		nextNumber: 0,
	}
}

// Broadcast receives ordering requests by clients and sends back an
// acknowledgement for each received message in order, indicating
// success or type of failure
func (b *broadcasterImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	b.once.Do(func() {
		// Send the genesis block to create the topic
		// otherwise consumers will throw an exception.
		b.sendBlock()
		go b.recvRequests(stream)
	})
	return b.cutBlock(b.config.General.BatchTimeout, b.config.General.BatchSize)
}

// Close shuts down the broadcast side of the orderer
func (b *broadcasterImpl) Close() error {
	if b.producer != nil {
		return b.producer.Close()
	}
	return nil
}

func (b *broadcasterImpl) sendBlock() error {
	block := &ab.Block{
		Messages: b.messages,
		Number:   b.nextNumber,
		PrevHash: b.prevHash,
	}
	Logger.Debugf("Prepared block %d with %d messages (%+v)", block.Number, len(block.Messages), block)

	b.messages = []*ab.BroadcastMessage{}
	b.nextNumber++
	hash, data := hashBlock(block)
	b.prevHash = hash

	return b.producer.Send(data)
}

func (b *broadcasterImpl) recvRequests(stream ab.AtomicBroadcast_BroadcastServer) {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err != nil {
			b.errChan <- err
			return
		}

		b.batchChan <- msg
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success

		if err := stream.Send(reply); err != nil {
			Logger.Info("Cannot send broadcast reply to client")
		} else {
			Logger.Debugf("Sent broadcast reply %v to client", reply.Status.String())
		}
	}
}

func (b *broadcasterImpl) cutBlock(period time.Duration, maxSize uint) error {
	every := time.NewTicker(period)

	for {
		select {
		case err := <-b.errChan:
			return err
		case msg := <-b.batchChan:
			b.messages = append(b.messages, msg)
			if len(b.messages) >= int(maxSize) {
				if err := b.sendBlock(); err != nil {
					return err
				}
			}
		case <-every.C:
			if len(b.messages) > 0 {
				if err := b.sendBlock(); err != nil {
					return err
				}
			}
		}
	}
}
