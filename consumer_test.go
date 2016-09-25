package orderer

import "testing"

func TestConsumerInitWrong(t *testing.T) {
	cases := []int64{oldestOffset - 1, newestOffset}

	for _, seek := range cases {
		mc, err := mockNewConsumer(t, config, seek)
		testClose(t, mc)
		if err == nil {
			t.Fatal("Consumer should have failed with out-of-range error")
		}
	}
}

func TestConsumerRecv(t *testing.T) {
	t.Run("oldest", testConsumerRecvFunc(oldestOffset, oldestOffset))
	t.Run("in-between", testConsumerRecvFunc(middleOffset, middleOffset))
	t.Run("newest", testConsumerRecvFunc(newestOffset-1, newestOffset-1))
}

func testConsumerRecvFunc(given, expected int64) func(t *testing.T) {
	return func(t *testing.T) {
		mc, err := mockNewConsumer(t, config, given)
		if err != nil {
			testClose(t, mc)
			t.Fatalf("Consumer should have proceeded normally: %s", err)
		}
		msg := <-mc.Recv()
		if (msg.Topic != config.Topic) ||
			msg.Partition != config.PartitionID ||
			msg.Offset != mc.(*mockConsumerImpl).consumedOffset ||
			msg.Offset != expected {
			t.Fatalf("Expected block %d, got %d", expected, msg.Offset)
		}
		testClose(t, mc)
	}
}
