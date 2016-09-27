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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	orderer "github.com/kchristidis/kafka-orderer"
	"github.com/kchristidis/kafka-orderer/ab"
	"github.com/kchristidis/kafka-orderer/config"
	"google.golang.org/grpc"
)

func main() {
	var kafkaVersion = sarama.V0_9_0_1 // TODO Ideally we'd set this in the YAML file but its type makes this impossible
	conf := config.Load()
	conf.Kafka.Version = kafkaVersion

	var loglevel string
	var verbose bool

	flag.StringVar(&loglevel, "loglevel", "info",
		"Set the logging level for the orderer library. (Allowed values: info, debug)")
	flag.BoolVar(&verbose, "verbose", false,
		"Turn on logging for the Kafka library. (Default: \"false\")")
	flag.Parse()

	orderer.SetLogLevel(loglevel)
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}

	ordererSrv := orderer.New(conf)
	defer ordererSrv.Teardown()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.General.ListenAddress, conf.General.ListenPort))
	if err != nil {
		panic(err)
	}
	rpcSrv := grpc.NewServer() // TODO Add TLS support
	ab.RegisterAtomicBroadcastServer(rpcSrv, ordererSrv)
	go rpcSrv.Serve(lis)

	// Trap SIGINT to trigger a shutdown
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	for {
		select {
		case <-signalChan:
			orderer.Logger.Info("Server shutting down")
			// rpcSrv.Stop()
			return
		}
	}
}
