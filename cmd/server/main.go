package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	orderer "github.com/kchristidis/kafka-orderer"
	"github.com/kchristidis/kafka-orderer/ab"
	"google.golang.org/grpc"
)

func main() {
	var brokers, loglevel string
	config := orderer.NewConfig()

	flag.StringVar(&brokers, "brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list.")
	flag.StringVar(&loglevel, "loglevel", "debug", "The logging level. (Allowed values: info, debug)")
	flag.StringVar(&config.Topic, "topic", "test", "The topic to publish/consume to/from.")
	flag.IntVar(&config.Port, "port", 6100, "The port to listen to for incoming RPCs.")
	flag.BoolVar(&config.Verbose, "verbose", false, "Turn on logging for the sarama library. (Default: \"false\")")
	flag.Parse() // TODO Validate user input

	config.Brokers = strings.Split(brokers, ",")
	config.SetLogLevel(loglevel)
	if config.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}

	ordererSrv := orderer.NewServer(config)
	defer ordererSrv.Close()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
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
			return
		}
	}
}
