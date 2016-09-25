/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
