package orderer

import (
	"io"

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

		// TODO Add block cutting logic here

		err = s.Send(msg.Data)
		if err != nil {
			return err
		}

		reply.Status = ab.Status_SUCCESS // TODO This shouldn't always be a success
		err = stream.Send(reply)
		if err != nil {
			return err
		}
		Logger.Debugf("Sent broadcast reply %v to client\n", reply.Status.String())
	}
}
