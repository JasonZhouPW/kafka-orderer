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

func TestSend(t *testing.T) {
	mp := mockNewProducer(t, &ConfigImpl{FlagsImpl: FlagsImpl{Topic: "test_topic"}})
	defer mp.Close()
	if err := mp.Send([]byte("hello world")); err != nil {
		t.Fatalf("Mock producer not functioning as expected")
	}
}
