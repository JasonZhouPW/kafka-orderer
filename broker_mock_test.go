package orderer

import (
	"testing"

	"github.com/Shopify/sarama"
)

type mockBrockerImpl struct {
	brokerImpl

	mockBroker *sarama.MockBroker
	handlerMap map[string]sarama.MockResponse
}

func mockNewBroker(t *testing.T, config *ConfigImpl) Broker {
	mockBroker := sarama.NewMockBroker(t, brokerID)
	handlerMap := make(map[string]sarama.MockResponse)
	// The sarama mock package doesn't allow us to return an error
	// for invalid offset requests, so we return an offset of -1.
	// Note that the mock offset responses below imply a broker with
	// newestOffset-1 blocks available. Therefore, if you are using this
	// broker as part of a bigger test where you intend to consume blocks,
	// make sure that the mockConsumer has been initialized accordingly
	// (Set the 'seek' parameter to newestOffset-1.)
	handlerMap["OffsetRequest"] = sarama.NewMockOffsetResponse(t).
		SetOffset(config.Topic, config.PartitionID, sarama.OffsetOldest, oldestOffset).
		SetOffset(config.Topic, config.PartitionID, sarama.OffsetNewest, newestOffset)
	mockBroker.SetHandlerByMap(handlerMap)

	broker := sarama.NewBroker(mockBroker.Addr())
	if err := broker.Open(nil); err != nil {
		t.Fatal("Cannot connect to mock broker:", err)
	}

	return &mockBrockerImpl{
		brokerImpl: brokerImpl{
			broker: broker,
			config: config,
		},
		mockBroker: mockBroker,
		handlerMap: handlerMap,
	}
}

func (mb *mockBrockerImpl) Close() error {
	mb.mockBroker.Close()
	return nil
}
