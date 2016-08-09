package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma-separated list")
	role    = flag.String("role", "p", "The client type ([p]roducer, [c]consumer)")
	topic   = flag.String("topic", "test", "The topic to publish/consume to/from")
	verbose = flag.Bool("verbose", true, "Turn on logging for the sarama library")

	duration = 5 * time.Second

	concurrentReqs = 1
	requiredAcks   = sarama.WaitForAll
	version        = sarama.V0_9_0_1

	pCount, mCount int
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Lshortfile)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	if strings.Compare(*role, "p") == 0 {
		producer := newProducer(brokerList, newConfig(concurrentReqs, requiredAcks, version))
		defer func() {
			if err := producer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()

		timer := time.NewTimer(duration)
		for {
			select {
			case <-timer.C:
				log.Printf("Timer expired\n")
				return
			default:
				sendMessage(producer, *topic)
				time.Sleep(1 * time.Second)
			}

		}
	}

	// TODO Add consumer
}

func newConfig(concurrentReqs int, requiredAcks sarama.RequiredAcks, version sarama.KafkaVersion) *sarama.Config {
	conf := sarama.NewConfig()
	conf.Net.MaxOpenRequests = concurrentReqs
	conf.Producer.RequiredAcks = requiredAcks
	conf.ClientID = *role + strconv.Itoa(pCount)
	conf.Version = version
	return conf
}

func newProducer(brokerList []string, config *sarama.Config) sarama.SyncProducer {
	pCount++
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln(err)
	}
	return producer
}

func sendMessage(producer sarama.SyncProducer, topic string) {
	mCount++
	newMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("msg " + strconv.Itoa(mCount))}
	partition, offset, err := producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
