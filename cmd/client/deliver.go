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

	for {
		select {
		case <-c.signalChan:
			err = stream.CloseSend()
			if err != nil {
				panic(fmt.Errorf("Failed to close the deliver stream: %v", err))
			}
			log.Println("Client shutting down.")
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
			panic(fmt.Errorf("Failed to receive a deliver reply from orderer: %v", err))
		}

		switch t := reply.GetType().(type) {
		case *ab.DeliverReply_Block:
			log.Println("Deliver reply from orderer (block): number ", t.Block.Number)
			count++
			if (count > 0) && (count%c.config.ack == 0) {
				updateAck.GetAcknowledgement().Number = t.Block.Number
				err = stream.Send(updateAck)
				if err != nil {
					log.Println("Failed to send ACK update to orderer: ", err)
				}
				log.Println("Sent ACK for block: number ", t.Block.Number)
			}
		case *ab.DeliverReply_Error:
			log.Printf("Deliver reply from orderer (error): %+v\n", t.Error.String())
		}
	}
}
