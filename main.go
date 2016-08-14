package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type flags struct {
	begin    int64
	brokers  string
	duration time.Duration
	group    string
	period   time.Duration
	role     string
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
	flag.StringVar(&userPrefs.group, "group", "1", "Consumer group to which the consumer will belong to")
	flag.DurationVar(&userPrefs.period, "period", 1*time.Second, "How often should the producer send a message")
	flag.StringVar(&userPrefs.role, "role", "", "The client type: [p]roducer, [c]consumer, [g]eneric client")
	flag.StringVar(&userPrefs.topic, "topic", "test", "The topic to publish/consume to/from")
	flag.BoolVar(&userPrefs.verbose, "verbose", false, "Turn on logging for the sarama library (default \"false\")")

	flag.Parse()

	if userPrefs.verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}
}

func main() {
	config := newConfig(&userPrefs)

	switch userPrefs.role {
	case "b":
		launchBoth(config, &userPrefs)
	case "p":
		launchProducer(config, &userPrefs)
	case "c":
		launchConsumer(config, &userPrefs)
	case "g":
		launchClient(config, &userPrefs)
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func newConfig(userPrefs *prefs) *cluster.Config {
	conf := cluster.NewConfig()
	conf.Net.MaxOpenRequests = userPrefs.concurrentReqs
	conf.Producer.RequiredAcks = userPrefs.requiredAcks
	conf.Version = userPrefs.version
	conf.Config.ClientID = userPrefs.role
	return conf
}
