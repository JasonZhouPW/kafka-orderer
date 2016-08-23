package orderer

import (
	"io"

	"github.com/kchristidis/kafka-orderer/ab"
)

// Broadcast receives ordering requests (i.e. messages that need to be ordered)
// by the client and sends back a reply of acknowledgement
// for each message in order indicating success or type of failure.
func (x *Server) Broadcast(stream ab.AtomicBroadcast_BroadcastServer) error {
	reply := new(ab.BroadcastReply)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		x.SendMessage(msg.Data)
		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
	}
}

// Deliver receives updates from the receiving client
// and adjusts the output of ordered messages accordingly.
func (x *Server) Deliver(stream ab.AtomicBroadcast_DeliverServer) error {
	for {
		upd, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch t := upd.GetType().(type) {
		case *ab.DeliverUpdate_Seek:
			return x.processSeek(stream, t)
		case *ab.DeliverUpdate_Acknowledgement:
			return x.processACK(stream, t)
		}
	}
}

func (x *Server) processSeek(stream ab.AtomicBroadcast_DeliverServer, msg *ab.DeliverUpdate_Seek) error {
	var seek int64
	window := int64(msg.Seek.WindowSize)
	switch msg.Seek.Start {
	case ab.SeekInfo_OLDEST:
		seek = getOffset(*x.Prefs, *x.Config, int64(-2))
	case ab.SeekInfo_NEWEST:
		seek = getOffset(*x.Prefs, *x.Config, int64(-1))
	case ab.SeekInfo_SPECIFIED:
		// TODO Assume for now that the number makes sense, will need to check it though
		seek = int64(ab.SeekInfo_SPECIFIED)
	}
	x.resetConsumer(seek, window)
	return nil
}

func (x *Server) processACK(stream ab.AtomicBroadcast_DeliverServer, msg *ab.DeliverUpdate_Acknowledgement) error {
	tokenChan, remaining := disablePush(x.TokenChan)

	newACK := int64(msg.Acknowledgement.Number)
	newTokenCount := newACK - x.LastACK + remaining
	x.LastACK = newACK

	x.TokenChan = enablePush(tokenChan, newTokenCount)
	return nil
}
