package orderer

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Server ...
type Server struct {
	Prefs    *PrefsImpl
	Config   *cluster.Config
	Producer sarama.SyncProducer

	Consumer  *cluster.Consumer
	TokenChan chan struct{}
	LastACK   int64
}

// NewServer ...
func NewServer(prefs *PrefsImpl) *Server {
	x := Server{
		Prefs:  prefs,
		Config: newConfig(prefs),
	}
	x.Producer = newProducer(x.Prefs, x.Config)
	x.SendMessage([]byte("init")) // send a message to create the topic, otherwise the consumer will throw an exception
	return &x
}

// SendMessage ...
func (x *Server) SendMessage(payload []byte) error {
	newMessage := &sarama.ProducerMessage{
		Topic: x.Prefs.Topic,
		Value: sarama.ByteEncoder(payload),
	}
	partition, offset, err := x.Producer.SendMessage(newMessage)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
		return err
	}
	fmt.Fprintf(os.Stdout, "Message \"%s\" sent to %v/%d at offset %d\n", payload, x.Prefs.Topic, partition, offset)
	return err
}

func newConfig(prefs *PrefsImpl) *cluster.Config {
	conf := cluster.NewConfig()
	conf.Net.MaxOpenRequests = prefs.ConcurrentReqs
	conf.Producer.RequiredAcks = prefs.RequiredAcks
	conf.Version = prefs.Version
	return conf
}

func newProducer(prefs *PrefsImpl, config *cluster.Config) sarama.SyncProducer {
	brokers := strings.Split(prefs.Brokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
	if err != nil {
		panic(err)
	}
	return producer
}

func newConsumer(prefs *PrefsImpl, config *cluster.Config) *cluster.Consumer {
	brokers := strings.Split(prefs.Brokers, ",")
	topics := []string{prefs.Topic}
	consumer, err := cluster.NewConsumer(brokers, prefs.Group, topics, config)
	if err != nil {
		panic(err)
	}
	return consumer
}

func (x *Server) resetConsumer(seek, window int64) {
	tokenChan, _ := disablePush(x.TokenChan)

	if x.Consumer != nil {
		if err := x.Consumer.Close(); err != nil {
			panic(err)
		}
	}
	x.Config.Consumer.Offsets.Initial = seek
	x.LastACK = seek - 1 // TODO For now this only works for a SPECIFIED Start
	x.Consumer = newConsumer(x.Prefs, x.Config)

	x.TokenChan = enablePush(tokenChan, window)
}

func disablePush(c chan struct{}) (chan struct{}, int64) {
	remaining := int64(len(c))
	log.Printf("Token channel had %d elements remaining\n", remaining)
	c = nil
	return c, remaining
}

func enablePush(c chan struct{}, newTokenCount int64) chan struct{} {
	c = make(chan struct{}, newTokenCount)
	for i := int64(0); i < newTokenCount; i++ {
		c <- struct{}{}
	}
	log.Printf("Token channel now has %d elements\n", len(c))
	return c
}

func getOffset(prefs PrefsImpl, config cluster.Config, begin int64) int64 {
	/* userPrefs.group += strconv.Itoa(rand.Intn(1000))      // create a random group
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // change Offsets.Initial
	consumer := newConsumer(&userPrefs, &config)
	message := <-consumer.Messages()
	offset := uint64(message.Offset)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}() */
	brokers := strings.Split(prefs.Brokers, ",")
	topics := []string{prefs.Topic}
	tempConsumer, err := sarama.NewConsumer(brokers, &config.Config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := tempConsumer.Close(); err != nil {
			panic(err)
		}
	}()
	prtCons, err := tempConsumer.ConsumePartition(topics[0], 0, begin)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := prtCons.Close(); err != nil {
			panic(err)
		}
	}()
	nextOffset := prtCons.HighWaterMarkOffset()
	log.Printf("Next offset is: %v\n", nextOffset)
	return nextOffset
}
