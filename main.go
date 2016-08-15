package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kchristidis/kafka-orderer/ab"
	"google.golang.org/grpc"
)

type flags struct {
	begin    int64
	brokers  string
	duration time.Duration
	group    string
	period   time.Duration
	port     int
	topic    string
	verbose  bool
}

type prefs struct {
	flags
	concurrentReqs int
	requiredAcks   sarama.RequiredAcks
	version        sarama.KafkaVersion
}

var userPrefs prefs

func init() {
	userPrefs = prefs{
		flags:          flags{},
		concurrentReqs: 1,
		requiredAcks:   sarama.WaitForAll,
		version:        sarama.V0_9_0_1,
	}

	flag.Int64Var(&userPrefs.begin, "begin", -2, "Offset the consumer should begin from, if available on the broker: -2 = oldest available, -1 = newest (ignore the past), >0: pick precise offset")
	flag.StringVar(&userPrefs.brokers, "brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	flag.DurationVar(&userPrefs.duration, "duration", 15*time.Second, "Time during which the producer will send messages")
	flag.StringVar(&userPrefs.group, "group", "0", "Consumer group to which the consumer will belong to")
	flag.DurationVar(&userPrefs.period, "period", 1*time.Second, "How often should the producer send a message")
	flag.IntVar(&userPrefs.port, "port", 6100, "gRPC port")
	flag.StringVar(&userPrefs.topic, "topic", "test", "The topic to publish/consume to/from")
	flag.BoolVar(&userPrefs.verbose, "verbose", false, "Turn on logging for the sarama library (default \"false\")")

	flag.Parse()
	// TODO Check for valid entries

	if userPrefs.verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	orderer := newOrderer(&userPrefs)
	defer func() {
		if err := orderer.producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	defer func() {
		if err := orderer.consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", userPrefs.port))
	if err != nil {
		log.Fatalln(err)
	}
	// TODO Add TLS support
	grpcServer := grpc.NewServer()
	ab.RegisterAtomicBroadcastServer(grpcServer, orderer)
	go grpcServer.Serve(lis)

	// Trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	total := time.NewTimer(userPrefs.duration)
	every := time.NewTicker(userPrefs.period)
	defer func() {
		every.Stop()
	}()
	for {
		select {
		case <-signals:
			return
		case <-total.C:
			log.Printf("Timer expired\n")
			return
		case <-every.C:
			orderer.sendMessage([]byte("foo"))
		case err := <-orderer.consumer.Errors():
			log.Printf("Error: %s\n", err.Error())
		case note := <-orderer.consumer.Notifications():
			log.Printf("Rebalanced: %+v\n", note)
		case message := <-orderer.consumer.Messages():
			fmt.Fprintf(os.Stdout, "Consumed message (topic: %s, part: %d, offset: %d, value: %s)\n",
				message.Topic, message.Partition, message.Offset, message.Value)
			orderer.consumer.MarkOffset(message, "")
		}
	}
}
