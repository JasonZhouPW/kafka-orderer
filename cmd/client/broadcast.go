package main

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"

	"github.com/kchristidis/kafka-orderer/ab"
	context "golang.org/x/net/context"
)

func (c *clientImpl) broadcast() {
	var random int
	message := &ab.BroadcastMessage{} // Has a Data field
	tokenChan := make(chan struct{}, c.config.count)

	stream, err := c.rpc.Broadcast(context.Background())
	if err != nil {
		panic(fmt.Errorf("Failed to invoke broadcast RPC: %v", err))
	}

	go c.recvBroadcastReplies(stream)

	for {
		select {
		case <-c.signalChan:
			err = stream.CloseSend()
			if err != nil {
				panic(fmt.Errorf("Failed to close the broadcast stream: %v", err))
			}
			logger.Info("Client shutting down")
			return
		case tokenChan <- struct{}{}:
			random = int((rand.Float32() + 1) * 49) // Get me a two digit integer.
			message.Data = []byte(strconv.Itoa(random))
			err := stream.Send(message)
			if err != nil {
				logger.Info("Failed to send broadcast message to orderer: ", err)
			}
			logger.Debugf("Sent broadcast message \"%s\" to orderer\n", message.Data)
		}
	}
}

func (c *clientImpl) recvBroadcastReplies(stream ab.AtomicBroadcast_BroadcastClient) {
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(fmt.Errorf("Failed to receive a broadcast reply from orderer: %v", err))
		}
		logger.Infof("Broadcast reply from orderer: %s", reply.Status.String())
	}
}
