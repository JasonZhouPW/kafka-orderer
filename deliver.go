package orderer

import (
	"io"

	"github.com/Shopify/sarama"
	"github.com/kchristidis/kafka-orderer/ab"
)

type deliverServerImpl struct {
	dyingChan, deadChan chan struct{}
	errChan             chan error
	updChan             chan *ab.DeliverUpdate
	tokenChan           chan struct{}

	config      *ConfigImpl
	kafkaConfig *sarama.Config
	consumer    *consumerImpl
	lastACK     int64
}

func newDeliverServer(config *ConfigImpl, kafkaConfig *sarama.Config) *deliverServerImpl {
	return &deliverServerImpl{
		dyingChan:   make(chan struct{}),
		deadChan:    make(chan struct{}),
		errChan:     make(chan error),
		updChan:     make(chan *ab.DeliverUpdate, 100), // TODO Size this properly
		config:      config,
		kafkaConfig: kafkaConfig,
	}
}

// Deliver receives updates from connected clients
// and adjusts the transmission of ordered messages to them accordingly.
// TODO Whenever this RPC is called, create a new consumer and tokenChan
func (s *ServerImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	var err error
	var upd *ab.DeliverUpdate

	ds := newDeliverServer(s.Config, s.kafkaConfig)
	defer ds.closeConsumer()

	go ds.recvReplies(stream)

	for {
		select {
		case <-s.DeadChan:
			close(s.DyingChan)
			return nil
		case err = <-ds.errChan:
			close(ds.dyingChan)
			<-ds.deadChan
			if err == io.EOF {
				return nil
			}
			return err
		case upd = <-ds.updChan:
			switch t := upd.GetType().(type) {
			case *ab.DeliverUpdate_Seek:
				err = ds.processSeek(t)
			case *ab.DeliverUpdate_Acknowledgement:
				err = ds.processACK(t)
			}
			if err != nil {
				Logger.Info("Failed to process received deliver message:", err)
			}
		case <-ds.tokenChan:
			select {
			case msg := <-ds.consumer.partition.Messages():
				reply := new(ab.DeliverReply)
				reply.Type = &ab.DeliverReply_Block{
					Block: &ab.Block{
						Number: uint64(msg.Offset),
						Messages: []*ab.BroadcastMessage{
							&ab.BroadcastMessage{
								Data: []byte(msg.Value),
							},
						},
					},
				}
				err := stream.Send(reply)
				if err != nil {
					close(ds.dyingChan)
					<-ds.deadChan
					return err
				}
				Logger.Debugf("Sent block %v with payload \"%s\" to client\n", msg.Offset, msg.Value)
			default:
				// Return the push token if there are no messages
				// available from the ordering service.
				ds.tokenChan <- struct{}{}
			}
		}
	}
}

func (ds *deliverServerImpl) recvReplies(stream ab.AtomicBroadcast_DeliverServer) {
	for {
		select {
		case <-ds.dyingChan:
			close(ds.deadChan)
			return
		default:
			upd, err := stream.Recv()
			if err != nil {
				ds.errChan <- err
			} else {
				ds.updChan <- upd
			}
		}
	}
}

func (ds *deliverServerImpl) processSeek(msg *ab.DeliverUpdate_Seek) error {
	var err error
	var seek, window int64
	Logger.Debug("Received SEEK message")

	window = int64(msg.Seek.WindowSize)
	Logger.Debug("Requested window size set to", window)

	switch msg.Seek.Start {
	case ab.SeekInfo_OLDEST:
		seek, err = getOffset(ds.config, int64(-2))
	case ab.SeekInfo_NEWEST:
		seek, err = getOffset(ds.config, int64(-1))
	case ab.SeekInfo_SPECIFIED:
		seek = int64(msg.Seek.SpecifiedNumber) // TODO Do not check for now and assume it is a valid offset number
	}

	if err != nil {
		return err
	}
	Logger.Debug("Requested seek number set to", seek)

	return ds.resetConsumer(seek, window)
}

func getOffset(config *ConfigImpl, beginFrom int64) (offset int64, err error) {
	broker := sarama.NewBroker(config.Brokers[0])
	err = broker.Open(nil)
	if err != nil {
		return
	}

	req := &sarama.OffsetRequest{}
	req.AddBlock(config.Topic, config.PartitionID, beginFrom, 1)
	resp, err := broker.GetAvailableOffsets(req)
	if err != nil {
		return
	}
	if err = broker.Close(); err != nil {
		return
	}

	return resp.GetBlock(config.Topic, config.PartitionID).Offsets[0], nil
}

func (ds *deliverServerImpl) disablePush() int64 {
	remTokens := int64(len(ds.tokenChan))
	ds.tokenChan = nil
	Logger.Debugf("Pushing blocks to client paused; found %v unused push token(s)", remTokens)
	return remTokens
}

func (ds *deliverServerImpl) enablePush(newTokenCount int64) {
	ds.tokenChan = make(chan struct{}, newTokenCount)
	for i := int64(0); i < newTokenCount; i++ {
		ds.tokenChan <- struct{}{}
	}
	Logger.Debugf("Pushing blocks to client resumed; %v push token(s) available", newTokenCount)
}

func (ds *deliverServerImpl) processACK(msg *ab.DeliverUpdate_Acknowledgement) error {
	Logger.Debug("Received ACK for block", msg.Acknowledgement.Number)
	remTokens := ds.disablePush()
	newACK := int64(msg.Acknowledgement.Number) // TODO Optionally mark this offset in Kafka
	newTokenCount := newACK - ds.lastACK + remTokens
	ds.lastACK = newACK
	ds.enablePush(newTokenCount)
	return nil
}
