package orderer

import (
	"io"
	"sync"
	"time"

	"github.com/kchristidis/kafka-orderer/ab"
)

// Broadcaster ...
type Broadcaster interface {
	Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error
	Close() error
}

type broadcasterImpl struct {
	producer Producer
	config   *ConfigImpl
	once     sync.Once

	batchChan  chan *ab.BroadcastMessage
	messages   []*ab.BroadcastMessage
	nextNumber uint64
	prevHash   []byte
}

func newBroadcaster(config *ConfigImpl) Broadcaster {
	return &broadcasterImpl{
		producer:   newProducer(config),
		config:     config,
		batchChan:  make(chan *ab.BroadcastMessage, config.Batch.Size),
		messages:   []*ab.BroadcastMessage{&ab.BroadcastMessage{Data: []byte("genesis")}},
		nextNumber: 0,
	}
}

// Broadcast receives ordering requests (i.e. messages that need to be ordered)
// by the client and sends back a reply of acknowledgement
// for each message in order indicating success or type of failure.
func (b *broadcasterImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	b.once.Do(func() {
		// Send the genesis block to create the topic
		// otherwise consumers will throw an exception.
		b.sendBlock()
		// Launch the goroutine that cuts blocks when appropriate.
		go b.cutBlock(b.config.Batch.Period, b.config.Batch.Size)
	})
	return b.recvReplies(stream)
}

// Close ...
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

func (b *broadcasterImpl) cutBlock(period time.Duration, maxSize int) {
	every := time.NewTicker(period)

	for {
		select {
		case msg := <-b.batchChan:
			b.messages = append(b.messages, msg)
			if len(b.messages) == maxSize {
				b.sendBlock()
			}
		case <-every.C:
			if len(b.messages) > 0 {
				b.sendBlock()
			}
		}
	}
}

func (b *broadcasterImpl) recvReplies(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.batchChan <- msg
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
		Logger.Debugf("Sent broadcast reply %v to client\n", reply.Status.String())
	}
}
