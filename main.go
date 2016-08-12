package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type flags struct {
	begin    int64
	brokers  string
	duration time.Duration
	role     string
	topic    string
	verbose  bool
}

var (
	concurrentReqs int
	requiredAcks   sarama.RequiredAcks
	version        sarama.KafkaVersion

	initFlags flags
)

func init() {
	concurrentReqs = 1
	requiredAcks = sarama.WaitForAll
	version = sarama.V0_9_0_1

	flag.Int64Var(&initFlags.begin, "begin", -2, "Offset the consumer should begin from, if available on the broker: -2 = oldest available, -1 = newest (ignore the past), >0: pick precise offset - default: -2")
	flag.StringVar(&initFlags.brokers, "brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list - default: localhost:9092")
	flag.DurationVar(&initFlags.duration, "duration", 15*time.Second, "Time during which the producer will send messages (at a rate of 1 msg/sec) - default: 15s")
	flag.StringVar(&initFlags.role, "role", "p", "The client type: [p]roducer, [c]consumer, [g]eneric client) - default: p")
	flag.StringVar(&initFlags.topic, "topic", "test", "The topic to publish/consume to/from - default: test")
	flag.BoolVar(&initFlags.verbose, "verbose", false, "Turn on logging for the sarama library - default: false")

	flag.Parse()

	if initFlags.verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	brokerList := strings.Split(initFlags.brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))
	config := newConfig(concurrentReqs, requiredAcks, version)

	switch initFlags.role {
	case "p":
		launchProducer(brokerList, config, initFlags)
	case "c":
		launchConsumer(brokerList, config, initFlags)
	case "g":
		launchClient(brokerList, config, initFlags)
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
