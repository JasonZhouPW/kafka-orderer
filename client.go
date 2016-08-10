package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func launchClient(brokerList []string, config *sarama.Config, role string) {
	client := newClient(brokerList, config, role)
	defer func() {
		if err := client.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	topics, err := client.Topics()
	if err != nil {
		panic(err)
	}
	// log.Printf("Topics: %v\n", topics)

	for i := range topics {
		log.Printf("Topic: %s", topics[i])
		pts, err := client.Partitions(topics[i])
		if err != nil {
			panic(err)
		}
		// log.Printf("\tPartitions: %v", pts)

		for j := range pts {
			log.Printf("\t- Partition: %v\n", pts[j])
			leader, err := client.Leader(topics[i], pts[j])
			if err != nil {
				panic(err)
			}
			log.Printf("\t\t- Leader: broker %v (%v)\n", leader.ID(), leader.Addr())
			oldest, err := client.GetOffset(topics[i], pts[j], sarama.OffsetOldest)
			if err != nil {
				panic(err)
			}
			upcoming, err := client.GetOffset(topics[i], pts[j], sarama.OffsetNewest)
			if err != nil {
				panic(err)
			}
			log.Printf("\t\t- Oldest available offset: %v\n", oldest)
			log.Printf("\t\t- Next offset: %v\n", upcoming)
		}

		pts, err = client.WritablePartitions(topics[i])
		if err != nil {
			panic(err)
		}
		log.Printf("\t- Writable partitions: %v\n", pts)
	}

}

func newClient(brokerList []string, config *sarama.Config, role string) sarama.Client {
	config.ClientID = role
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		panic(err)
	}
	return client
}
