package orderer

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/kchristidis/kafka-orderer/ab"
)

// Broadcast receives ordering requests (i.e. messages that need to be ordered)
// by the client and sends back a reply of acknowledgement
// for each message in order indicating success or type of failure.
func (s *ServerImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.SendMessage(msg.Data)
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
	}
}

// Deliver receives updates from the receiving client
// and adjusts the output of ordered messages accordingly.
func (s *ServerImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	var err error
	var upd *ab.DeliverUpdate

	dying := make(chan struct{}, 1)
	dead := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	recvChan := make(chan *ab.DeliverUpdate, 1) // TODO Make sure this capacity is the right one

	go func() {
		for {
			select {
			case <-dying:
				dead <- struct{}{}
				return
			default:
				upd, err := stream.Recv()
				if err != nil {
					errChan <- err
				} else {
					recvChan <- upd
				}
			}
		}
	}()

	for {
		select {
		case upd = <-recvChan:
			switch t := upd.GetType().(type) {
			case *ab.DeliverUpdate_Seek:
				s.processSeek(t)
			case *ab.DeliverUpdate_Acknowledgement:
				s.processACK(t)
			}
		case <-s.TokenChan:
			select {
			case message := <-s.Consumer.Partition.Messages():
				fmt.Fprintf(os.Stdout, "Consumed message (topic: %s, part: %d, offset: %d, value: %s)\n",
					message.Topic, message.Partition, message.Offset, message.Value)
				reply := new(ab.DeliverReply)
				reply.Type = &ab.DeliverReply_Block{
					Block: &ab.Block{
						Number: uint64(message.Offset),
						Messages: []*ab.BroadcastMessage{
							&ab.BroadcastMessage{
								Data: []byte(message.Value),
							},
						},
					},
				}
				err := stream.Send(reply)
				if err != nil {
					dying <- struct{}{}
					<-dead
					return err
				}
			default:
				s.TokenChan <- struct{}{}
			}
		case err = <-errChan:
			if err == io.EOF {
				dying <- struct{}{}
				<-dead
				return nil
			}
			if err != nil {
				dying <- struct{}{}
				<-dead
				return err
			}
		}
	}
}

func (s *ServerImpl) processSeek(msg *ab.DeliverUpdate_Seek) {
	var seek int64
	window := int64(msg.Seek.WindowSize)
	switch msg.Seek.Start {
	case ab.SeekInfo_OLDEST:
		seek = getOffset(s.Prefs, int64(-2))
	case ab.SeekInfo_NEWEST:
		seek = getOffset(s.Prefs, int64(-1))
	case ab.SeekInfo_SPECIFIED:
		seek = int64(msg.Seek.SpecifiedNumber) // TODO Assume for now that this is a valid offset number
	}
	s.resetConsumer(seek, window)
}

func (s *ServerImpl) processACK(msg *ab.DeliverUpdate_Acknowledgement) {
	tokenChan, remaining := disablePush(s.TokenChan)
	newACK := int64(msg.Acknowledgement.Number)
	// TODO Mark this offset in Kafka
	newTokenCount := newACK - s.lastACK + remaining
	s.lastACK = newACK
	s.TokenChan = enablePush(tokenChan, newTokenCount)
}

func (s *ServerImpl) resetConsumer(seek, window int64) {
	tokenChan, _ := disablePush(s.TokenChan)
	s.CloseConsumer()
	s.lastACK = seek - 1
	s.Consumer = newConsumer(s.Prefs, s.Config, seek)
	s.TokenChan = enablePush(tokenChan, window)
}

func getOffset(prefs *PrefsImpl, beginFrom int64) int64 {
	broker := sarama.NewBroker(strings.Split(prefs.Brokers, ",")[0])
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}
	req := &sarama.OffsetRequest{}
	req.AddBlock(prefs.Topic, 0, beginFrom, 1)
	resp, err := broker.GetAvailableOffsets(req)
	if err != nil {
		panic(err)
	}
	if err = broker.Close(); err != nil {
		panic(err)
	}
	return resp.GetBlock(prefs.Topic, prefs.PartitionID).Offsets[0]
}
