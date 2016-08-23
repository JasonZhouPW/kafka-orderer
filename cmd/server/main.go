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
	flag.StringVar(&prefs.Group, "group", "100", "Consumer group to which the consumer will belong to")
	flag.IntVar(&prefs.Port, "port", 6100, "gRPC port")
	flag.BoolVar(&prefs.Verbose, "verbose", false, "Turn on logging for the sarama library (default \"false\")")

	flag.Parse() // TODO Check for valid entries

	if prefs.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	orderer := orderer.NewServer(prefs)
	defer func() {
		if err := orderer.Producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	defer func() {
		if orderer.Consumer != nil {
			if err := orderer.Consumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}
	}()

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
		/* case err := <-orderer.Consumer.Errors():
			log.Printf("Error: %s\n", err.Error())
		case note := <-orderer.Consumer.Notifications():
			log.Printf("Rebalanced: %+v\n", note) */
		case <-orderer.TokenChan:
			message := <-orderer.Consumer.Messages()
			fmt.Fprintf(os.Stdout, "Consumed message (topic: %s, part: %d, offset: %d, value: %s)\n",
				message.Topic, message.Partition, message.Offset, message.Value)
			orderer.Consumer.MarkOffset(message, "")
		}
	}
}
