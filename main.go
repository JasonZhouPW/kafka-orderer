package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
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

	pCount, cCount, outCount, inCount int
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
		pCount++
		config.ClientID = *role + strconv.Itoa(pCount)
		client := newProducer(brokerList, config)
		defer func() {
			if err := client.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
		// send messages for 'duration'
		timer := time.NewTimer(duration)
		for {
			select {
			case <-timer.C:
				log.Printf("Timer expired\n")
				return
			default:
				sendMessage(client, *topic, outCount)
				time.Sleep(1 * time.Second)
			}

		}
	case "c":
		cCount++
		config.ClientID = *role + strconv.Itoa(cCount)
		client := newConsumer(brokerList, config)
		defer func() {
			if err := client.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
		topics, err := client.Topics()
		if err != nil {
			panic(err)
		}
		log.Printf("Retrieved topics: %v", topics)
		for i := range topics {
			partitions, err := client.Partitions(topics[i])
			if err != nil {
				panic(err)
			}
			log.Printf("Retrieved partitions for topic %v: %v", topics[i], partitions)
		}
		pConsumer, err := client.ConsumePartition("test", 0, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := pConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
		// Trap SIGINT to trigger a shutdown
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		for {
			select {
			case <-signals:
				break
			case message := <-pConsumer.Messages():
				inCount++
				log.Printf("Consumed message offset %d\n", message.Offset)
			}
		}
	default:
		log.Fatalf("Role should have been either [p]roducer or [c]consumer, instead got %v", *role)
	}
}

func newConfig(concurrentReqs int, requiredAcks sarama.RequiredAcks, version sarama.KafkaVersion) *sarama.Config {
	conf := sarama.NewConfig()
	conf.Net.MaxOpenRequests = concurrentReqs
	conf.Producer.RequiredAcks = requiredAcks
	conf.Version = version
	return conf
}

func newProducer(brokerList []string, config *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}
	return producer
}

func newConsumer(brokerList []string, config *sarama.Config) sarama.Consumer {
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	return consumer
}

func sendMessage(producer sarama.SyncProducer, topic string, count int) {
	count = outCount
	newMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("msg " + strconv.Itoa(count))}
	partition, offset, err := producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
