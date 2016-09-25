package orderer

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestBrokerGetOffset(t *testing.T) {
	t.Run("oldest", testBrokerGetOffsetFunc(sarama.OffsetOldest, oldestOffset))
	t.Run("newest", testBrokerGetOffsetFunc(sarama.OffsetNewest, newestOffset))
}

func testBrokerGetOffsetFunc(given, expected int64) func(t *testing.T) {
	return func(t *testing.T) {
		mb := mockNewBroker(t, config)
		defer testClose(t, mb)

		offset, _ := mb.GetOffset(newOffsetReq(mb.(*mockBrockerImpl).config, given))
		if offset != expected {
			t.Fatalf("Expected offset %d, got %d instead", expected, offset)
		}
	}
}
