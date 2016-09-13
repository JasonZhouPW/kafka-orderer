package orderer

import (
	"testing"

	"github.com/Shopify/sarama/mocks"
)

type mockProducerImpl struct {
	checker  mocks.ValueChecker
	producer *mocks.SyncProducer
	topic    string
}

func mockNewProducer(t *testing.T, config *ConfigImpl) Producer {
	return &mockProducerImpl{
		checker:  nil,
		producer: mocks.NewSyncProducer(t, nil),
		topic:    config.Topic,
	}
}

func (mp *mockProducerImpl) Send(payload []byte) error {
	mp.producer.ExpectSendMessageWithCheckerFunctionAndSucceed(mp.checker)
	_, _, err := mp.producer.SendMessage(newMsg(payload, mp.topic))
	return err
}

func (mp *mockProducerImpl) Close() error {
	return mp.producer.Close()
}
