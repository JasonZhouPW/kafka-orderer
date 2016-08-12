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
	period   time.Duration
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

	flag.Int64Var(&initFlags.begin, "begin", -2, "Offset the consumer should begin from, if available on the broker: -2 = oldest available, -1 = newest (ignore the past), >0: pick precise offset")
	flag.StringVar(&initFlags.brokers, "brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	flag.DurationVar(&initFlags.duration, "duration", 15*time.Second, "Time during which the producer will send messages")
	flag.DurationVar(&initFlags.period, "period", 1*time.Second, "How often should the producer send a message")
	flag.StringVar(&initFlags.role, "role", "", "The client type: [p]roducer, [c]consumer, [g]eneric client")
	flag.StringVar(&initFlags.topic, "topic", "test", "The topic to publish/consume to/from")
	flag.BoolVar(&initFlags.verbose, "verbose", false, "Turn on logging for the sarama library (default \"false\")")

	flag.Parse()

	if initFlags.verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	brokerList := strings.Split(initFlags.brokers, ",")
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
