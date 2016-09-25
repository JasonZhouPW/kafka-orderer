package main

import (
	"fmt"
	"io"
	"log"

	"github.com/kchristidis/kafka-orderer/ab"
	context "golang.org/x/net/context"
)

func (c *clientImpl) deliver() {
	updateSeek := &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Seek{
			Seek: &ab.SeekInfo{
				WindowSize: uint64(c.config.window),
			},
		},
	}

	switch c.config.seek {
	case -2:
		updateSeek.GetSeek().Start = ab.SeekInfo_OLDEST
	case -1:
		updateSeek.GetSeek().Start = ab.SeekInfo_NEWEST
	default:
		updateSeek.GetSeek().Start = ab.SeekInfo_SPECIFIED
		updateSeek.GetSeek().SpecifiedNumber = uint64(c.config.seek)
	}

	stream, err := c.rpc.Deliver(context.Background())
	if err != nil {
		panic(fmt.Errorf("Failed to invoke deliver RPC: %v", err))
	}

	go c.recvDeliverReplies(stream)

	err = stream.Send(updateSeek)
	if err != nil {
		log.Println("Failed to send seek update to orderer: ", err)
	}
	logger.Debugf("Sent seek message (start: %v, number: %v, window: %v) to orderer\n",
		updateSeek.GetSeek().Start, updateSeek.GetSeek().SpecifiedNumber, updateSeek.GetSeek().WindowSize)

	for {
		select {
		case <-c.signalChan:
			err = stream.CloseSend()
			if err != nil {
				panic(fmt.Errorf("Failed to close the deliver stream: %v", err))
			}
			logger.Info("Client shutting down")
			return
		}
	}
}

func (c *clientImpl) recvDeliverReplies(stream ab.AtomicBroadcast_DeliverClient) {
	var count int
	updateAck := &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Acknowledgement{
			Acknowledgement: &ab.Acknowledgement{}, // Has a Number field
		},
	}

	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}

		switch t := reply.GetType().(type) {
		case *ab.DeliverReply_Block:
			logger.Infof("Deliver reply from orderer: block %v, payload %v, prevHash %v",
				t.Block.Number, t.Block.Messages, t.Block.PrevHash)
			count++
			if (count > 0) && (count%c.config.ack == 0) {
				updateAck.GetAcknowledgement().Number = t.Block.Number
				err = stream.Send(updateAck)
				if err != nil {
					logger.Info("Failed to send ACK update to orderer: ", err)
				}
				logger.Debugf("Sent ACK for block %d", t.Block.Number)
			}
		case *ab.DeliverReply_Error:
			logger.Info("Deliver reply from orderer:", t.Error.String())
		}
	}
}
