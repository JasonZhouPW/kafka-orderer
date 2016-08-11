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
	begin    = flag.Int64("begin", -2, "Offset the consumer should begin from, if available on the broker: -2 = oldest available, -1 = newest (ignore the past), >0: pick precise offset - default: -2")
	brokers  = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list - default: localhost:9092")
	duration = flag.Duration("duration", 15*time.Second, "Time during which the producer will send messages (at a rate of 1 msg/sec) - default: 15s")
	role     = flag.String("role", "p", "The client type: [p]roducer, [c]consumer, [g]eneric client) - default: p")
	topic    = flag.String("topic", "test", "The topic to publish/consume to/from - default: test")
	verbose  = flag.Bool("verbose", false, "Turn on logging for the sarama library - default: false")

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
		launchProducer(brokerList, config, *role, *duration)
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
