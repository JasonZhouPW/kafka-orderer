package main

import (
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/kchristidis/kafka-orderer/ab"
	context "golang.org/x/net/context"
)

func (c *clientImpl) broadcast() {
	var count int
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
			log.Println("Client shutting down.")
			return
		case tokenChan <- struct{}{}:
			count++
			message.Data = []byte(strconv.Itoa(count))
			err := stream.Send(message)
			if err != nil {
				log.Println("Failed to send broadcast message to ordererer: ", err)
			}
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
		log.Println("Broadcast reply from orderer: ", reply.Status.String())
	}
}
