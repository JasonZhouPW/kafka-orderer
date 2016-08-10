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
	begin    = flag.Int64("begin", -2, "Offset the consumer should begin from, if available on the broker: -2 = oldest available, -1 = newest (ignore the past), >0: pick precise offset")
	brokers  = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	duration = flag.Int("duration", 15, "Number of seconds for which the producer will send messages")
	role     = flag.String("role", "p", "The client type: [p]roducer, [c]consumer, [g]eneric client)")
	topic    = flag.String("topic", "test", "The topic to publish/consume to/from")
	verbose  = flag.Bool("verbose", false, "Turn on logging for the sarama library")

	concurrentReqs = 1
	requiredAcks   = sarama.WaitForAll

	version = sarama.V0_9_0_1
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
		sendDuration := time.Duration(*duration) * time.Second
		launchProducer(brokerList, config, *role, sendDuration)
	case "c":
		startFromOffset := *begin
		log.Printf("Chosen offset: %v\n", startFromOffset)
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
