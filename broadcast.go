package orderer

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/kchristidis/kafka-orderer/ab"
)

type broadcastServerImpl struct {
	parent     *serverImpl
	producer   sarama.SyncProducer
	batchChan  chan *ab.BroadcastMessage
	messages   []*ab.BroadcastMessage
	nextNumber uint64
	prevHash   []byte
}

func newBroadcastServer(s *serverImpl) *broadcastServerImpl {
	bs := &broadcastServerImpl{
		parent:     s,
		producer:   newProducer(s.config),
		batchChan:  make(chan *ab.BroadcastMessage, s.config.Batch.Size),
		messages:   []*ab.BroadcastMessage{&ab.BroadcastMessage{Data: []byte("genesis")}},
		nextNumber: 0,
	}
	// Send the genesis block to create the topic
	// otherwise consumers will throw an exception.
	bs.sendBlock()
	go bs.checkForBlock()

	return bs
}

func newProducer(config *ConfigImpl) sarama.SyncProducer {
	brokerConfig := newBrokerConfig(config)
	producer, err := sarama.NewSyncProducer(config.Brokers, brokerConfig)
	if err != nil {
		panic(fmt.Errorf("Failed to create Kafka producer: %v", err))
	}
	return producer
}

func (bs *broadcastServerImpl) sendBlock() error {
	block := &ab.Block{
		Messages: bs.messages,
		Number:   bs.nextNumber,
		PrevHash: bs.prevHash,
	}
	Logger.Debugf("Prepared block %d with %d messages (%+v)", block.Number, len(block.Messages), block)

	bs.messages = []*ab.BroadcastMessage{}
	bs.nextNumber++
	hash, data := hashBlock(block)
	bs.prevHash = hash

	err := bs.send(data)
	if err != nil {
		return err
	}

	return nil
}

func hashBlock(block *ab.Block) (hash, data []byte) {
	data, err := proto.Marshal(block)
	if err != nil {
		panic(fmt.Errorf("Failed to marshal block: %v", err))
	}

	hash = make([]byte, 64)
	sha3.ShakeSum256(hash, data)
	return
}

func (bs *broadcastServerImpl) send(payload []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: bs.parent.config.Topic,
		Value: sarama.ByteEncoder(payload),
	}
	_, offset, err := bs.producer.SendMessage(msg)
	if err == nil {
		Logger.Debugf("Forwarded block to ordering service (number: %v, offset: %v)\n", bs.nextNumber-1, offset)
	}
	return err
}

func (bs *broadcastServerImpl) checkForBlock() {
	every := time.NewTicker(bs.parent.config.Batch.Period)
	maxSize := bs.parent.config.Batch.Size

	for {
		select {
		case msg := <-bs.batchChan:
			bs.messages = append(bs.messages, msg)
			if len(bs.messages) == maxSize {
				bs.sendBlock()
			}
		case <-every.C:
			if len(bs.messages) > 0 {
				bs.sendBlock()
			}
		}
	}
}

// Broadcast receives ordering requests (i.e. messages that need to be ordered)
// by the client and sends back a reply of acknowledgement
// for each message in order indicating success or type of failure.
func (s *serverImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.broadcaster.batchChan <- msg
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
		Logger.Debugf("Sent broadcast reply %v to client\n", reply.Status.String())
	}
}
