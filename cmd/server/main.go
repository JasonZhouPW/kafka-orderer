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
	"google.golang.org/grpc"
)

var prefs *orderer.PrefsImpl

func init() {
	prefs = orderer.NewPrefs()

	flag.StringVar(&prefs.Brokers, "brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	flag.StringVar(&prefs.Topic, "topic", "test", "The topic to publish/consume to/from")
	flag.IntVar(&prefs.Port, "port", 6100, "gRPC port")
	flag.BoolVar(&prefs.Verbose, "verbose", false, "Turn on logging for the sarama library (default \"false\")")

	flag.Parse() // TODO Check for valid entries

	if prefs.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	orderer := orderer.NewServer(prefs)
	defer orderer.CloseProducer()
	defer orderer.CloseConsumer()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", orderer.Prefs.Port))
	if err != nil {
		log.Fatalln(err)
	}
	// TODO Add TLS support
	grpcServer := grpc.NewServer()
	ab.RegisterAtomicBroadcastServer(grpcServer, orderer)
	go grpcServer.Serve(lis)

	// Trap SIGINT to trigger a shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	for {
		select {
		case <-signalChan:
			return
		}
	}
}
