package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	role    = flag.String("role", "p", "The client type: [p]roducer, [c]consumer, [g]eneric client, [b]roker)")
	topic   = flag.String("topic", "test", "The topic to publish/consume to/from")
	verbose = flag.Bool("verbose", true, "Turn on logging for the sarama library")

	sendDuration    = 15 * time.Second
	startFromOffset = sarama.OffsetOldest

	concurrentReqs = 1
	requiredAcks   = sarama.WaitForAll
	version        = sarama.V0_9_0_1
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := newConfig(concurrentReqs, requiredAcks, version)

	switch *role {
	case "p":
		launchProducer(brokerList, config, *role, sendDuration)
	case "c":
		launchConsumer(brokerList, config, *role, startFromOffset)
	case "g":
		launchClient(brokerList, config, *role)
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func newConfig(concurrentReqs int, requiredAcks sarama.RequiredAcks, version sarama.KafkaVersion) *sarama.Config {
	conf := sarama.NewConfig()
	conf.Net.MaxOpenRequests = concurrentReqs
	conf.Producer.RequiredAcks = requiredAcks
	conf.Version = version
	return conf
}
