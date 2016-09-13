package orderer

import (
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/kchristidis/kafka-orderer/ab"
)

// ClientDeliverer ...
type ClientDeliverer interface {
	Deliverer
	ResetConsumer(seek int64) (Consumer, error)
}

type clientDelivererImpl struct {
	consumer Consumer
	config   *ConfigImpl
	deadChan chan struct{}

	errChan   chan error
	updChan   chan *ab.DeliverUpdate
	tokenChan chan struct{}
	lastACK   int64
}

func newClientDeliverer(config *ConfigImpl, deadChan chan struct{}) ClientDeliverer {
	return &clientDelivererImpl{
		config:   config,
		deadChan: deadChan,
		errChan:  make(chan error),
		updChan:  make(chan *ab.DeliverUpdate, 100), // TODO Size this properly
	}
}

// Deliver ...
func (cd *clientDelivererImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	go cd.recvReplies(stream)
	return cd.getBlocks(stream)
}

// ResetConsumer ...
func (cd *clientDelivererImpl) ResetConsumer(seek int64) (Consumer, error) {
	return newConsumer(cd.config, seek)
}

// Close ...
func (cd *clientDelivererImpl) Close() error {
	if cd.consumer != nil {
		return cd.consumer.Close()
	}
	return nil
}

func (cd *clientDelivererImpl) recvReplies(stream ab.AtomicBroadcast_DeliverServer) {
	for {
		upd, err := stream.Recv()
		if err != nil {
			cd.errChan <- err
		} else {
			cd.updChan <- upd
		}
	}
}

func (cd *clientDelivererImpl) getBlocks(stream ab.AtomicBroadcast_DeliverServer) error {
	var err error
	var upd *ab.DeliverUpdate
	block := &ab.Block{}
	for {
		select {
		case <-cd.deadChan:
			return nil
		case err = <-cd.errChan:
			if err == io.EOF {
				return nil
			}
			return err
		case upd = <-cd.updChan:
			switch t := upd.GetType().(type) {
			case *ab.DeliverUpdate_Seek:
				err = cd.processSeek(t)
			case *ab.DeliverUpdate_Acknowledgement:
				err = cd.processACK(t)
			}
			if err != nil {
				Logger.Info("Failed to process received deliver message:", err)
			}
		case <-cd.tokenChan:
			select {
			case data := <-cd.consumer.Recv():
				err := proto.Unmarshal(data.Value, block)
				if err != nil {
					Logger.Info("Failed to unmarshal retrieved block from ordering service:", err)
				}
				reply := new(ab.DeliverReply)
				reply.Type = &ab.DeliverReply_Block{Block: block}
				err = stream.Send(reply)
				if err != nil {
					return err
				}
				Logger.Debugf("Sent block %v to client (prevHash: %v, messages: %v)\n",
					block.Number, block.PrevHash, block.Messages)
			default:
				// Return the push token if there are no messages
				// available from the ordering service.
				cd.tokenChan <- struct{}{}
			}
		}
	}
}

func (cd *clientDelivererImpl) processACK(msg *ab.DeliverUpdate_Acknowledgement) error {
	Logger.Debug("Received ACK for block", msg.Acknowledgement.Number)
	remTokens := cd.disablePush()
	newACK := int64(msg.Acknowledgement.Number) // TODO Optionally mark this offset in Kafka
	newTokenCount := newACK - cd.lastACK + remTokens
	cd.lastACK = newACK
	cd.enablePush(newTokenCount)
	return nil
}

func (cd *clientDelivererImpl) disablePush() int64 {
	// No need to add a lock to ensure these operations happen atomically.
	// The caller is the only function that can modify the tokenChan.
	remTokens := int64(len(cd.tokenChan))
	cd.tokenChan = nil
	Logger.Debugf("Pushing blocks to client paused; found %v unused push token(s)", remTokens)
	return remTokens
}

func (cd *clientDelivererImpl) enablePush(newTokenCount int64) {
	cd.tokenChan = make(chan struct{}, newTokenCount)
	for i := int64(0); i < newTokenCount; i++ {
		cd.tokenChan <- struct{}{}
	}
	Logger.Debugf("Pushing blocks to client resumed; %v push token(s) available", newTokenCount)
}

func (cd *clientDelivererImpl) processSeek(msg *ab.DeliverUpdate_Seek) error {
	var err error
	var seek, window int64
	Logger.Debug("Received SEEK message")

	window = int64(msg.Seek.WindowSize)
	Logger.Debug("Requested window size set to", window)

	switch msg.Seek.Start {
	case ab.SeekInfo_OLDEST:
		seek, err = getOffset(cd.config, int64(-2))
	case ab.SeekInfo_NEWEST:
		seek, err = getOffset(cd.config, int64(-1))
	case ab.SeekInfo_SPECIFIED:
		seek = int64(msg.Seek.SpecifiedNumber) // TODO Do not check for now and assume it is a valid offset number
	}

	if err != nil {
		return err
	}
	Logger.Debug("Requested seek number set to", seek)

	cd.disablePush()
	if err := cd.Close(); err != nil {
		return err
	}
	cd.lastACK = seek - 1
	Logger.Debug("Set last ACK for this client's consumer to", cd.lastACK)

	cd.consumer, err = cd.ResetConsumer(seek)
	if err != nil {
		return err
	}

	cd.enablePush(window)
	return nil
}
