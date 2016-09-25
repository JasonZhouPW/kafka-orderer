package orderer

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama/mocks"
)

type mockProducerImpl struct {
	config   *ConfigImpl
	producer *mocks.SyncProducer

	checker        mocks.ValueChecker
	disk           chan []byte // This is the "disk"/log that the producer writes to
	producedOffset int64
	t              *testing.T
}

func mockNewProducer(t *testing.T, config *ConfigImpl, seek int64, disk chan []byte) Producer {
	mp := &mockProducerImpl{
		config:         config,
		producer:       mocks.NewSyncProducer(t, nil),
		checker:        nil,
		disk:           disk,
		producedOffset: 0,
		t:              t,
	}
	if seek >= oldestOffset && seek <= (newestOffset-1) {
		mp.testFillWithBlocks(seek - 1) // Prepare the producer so that the next Send gives you block "seek"
	} else {
		panic(fmt.Errorf("Out of range seek number given to producer"))
	}
	return mp
}

func (mp *mockProducerImpl) Send(payload []byte) error {
	mp.producer.ExpectSendMessageWithCheckerFunctionAndSucceed(mp.checker)
	mp.producedOffset++
	mp.disk <- payload
	prt, ofs, err := mp.producer.SendMessage(newMsg(payload, mp.config.Topic))
	if err != nil ||
		prt != mp.config.PartitionID ||
		ofs != mp.producedOffset {
		mp.t.Fatal("Producer not functioning as expected")
	}
	return err
}

func (mp *mockProducerImpl) Close() error {
	return mp.producer.Close()
}

func (mp *mockProducerImpl) testFillWithBlocks(seek int64) {
	dyingChan := make(chan struct{})
	deadChan := make(chan struct{})

	go func() { // This goroutine is meant to read only the "fill-in" blocks.
		for {
			select {
			case <-mp.disk:
			case <-dyingChan:
				close(deadChan)
				return
			}
		}
	}()

	for i := int64(1); i <= seek; i++ {
		mp.Send([]byte("fill-in"))
	}

	close(dyingChan)
	<-deadChan
	return
}
