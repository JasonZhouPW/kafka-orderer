package main

import (
	"io"

	"github.com/kchristidis/kafka-orderer/ab"
)

// Broadcast receives ordering requests (i.e. messages that need to be ordered)
// by the client and sends back a reply of acknowledgement
// for each message in order indicating success or type of failure.
func (x *ordererImpl) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		x.sendMessage(msg.Data)
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
	}
}

// Deliver receives updates from the receiving client
// and adjusts the output of ordered messages accordingly.
func (x *ordererImpl) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	for {
		upd, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		props := upd.GetNewProperties()
		switch props.Start {
		case ab.Properties_OLDEST:

		case ab.Properties_NEWEST:

		case ab.Properties_SPECIFIED:

		default:
			return nil
		}
	}
}
