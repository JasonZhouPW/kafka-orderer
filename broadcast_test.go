package orderer

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kchristidis/kafka-orderer/ab"
)

func TestBroadcastInit(t *testing.T) {
	disk := make(chan []byte)

	mb := mockNewBroadcaster(t, config, oldestOffset, disk)
	defer testClose(t, mb)

	mbs := newMockBroadcastStream(t)
	go func() {
		if err := mb.Broadcast(mbs); err != nil {
			t.Fatal("Broadcast error:", err)
		}
	}()

	for {
		select {
		case in := <-disk:
			block := new(ab.Block)
			err := proto.Unmarshal(in, block)
			if err != nil {
				t.Fatal("Expected a block on the broker's disk")
			}
			if !(bytes.Equal(block.GetMessages()[0].Data, []byte("checkpoint"))) {
				t.Fatal("Expected first block to be a checkpoint")
			}
			return
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Should have received the initialization block by now")
		}
	}
}

func TestBroadcastReply(t *testing.T) {
	disk := make(chan []byte)

	mb := mockNewBroadcaster(t, config, oldestOffset, disk)
	defer testClose(t, mb)

	mbs := newMockBroadcastStream(t)
	go func() {
		if err := mb.Broadcast(mbs); err != nil {
			t.Fatal("Broadcast error:", err)
		}
	}()

	<-disk // We tested the checkpoint block in a previous test, so we can ignore it now

	// Send a message to the orderer
	go func() {
		mbs.incoming <- &ab.BroadcastMessage{Data: []byte("single message")}
	}()

	for {
		select {
		case reply := <-mbs.outgoing:
			if reply.Status != ab.Status_SUCCESS {
				t.Fatal("Client should have received a SUCCESS reply")
			}
			return
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Should have received a broadcast reply by the orderer by now")
		}
	}
}

func TestBroadcastBatch(t *testing.T) {
	disk := make(chan []byte)

	mb := mockNewBroadcaster(t, config, oldestOffset, disk)
	defer testClose(t, mb)

	mbs := newMockBroadcastStream(t)
	go func() {
		if err := mb.Broadcast(mbs); err != nil {
			t.Fatal("Broadcast error:", err)
		}
	}()

	<-disk // We tested the checkpoint block in a previous test, so we can ignore it now

	// Pump a batch's worth of messages into the system
	go func() {
		for i := 0; i < config.Batch.Size; i++ {
			mbs.incoming <- &ab.BroadcastMessage{Data: []byte("message " + strconv.Itoa(i))}
		}
	}()

	// Ignore the broadcast replies as they have been tested elsewhere
	for i := 0; i < config.Batch.Size; i++ {
		<-mbs.outgoing
	}

	for {
		select {
		case in := <-disk:
			block := new(ab.Block)
			err := proto.Unmarshal(in, block)
			if err != nil {
				t.Fatal("Expected a block on the broker's disk")
			}
			if len(block.Messages) != config.Batch.Size {
				t.Fatalf("Expected block to have %d messages instead of %d", config.Batch.Size, len(block.Messages))
			}
			return
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Should have received the initialization block by now")
		}
	}
}

func TestBroadcastClose(t *testing.T) {
	errChan := make(chan error)

	mb := mockNewBroadcaster(t, config, oldestOffset, make(chan []byte))
	mbs := newMockBroadcastStream(t)
	go func() {
		if err := mb.Broadcast(mbs); err != nil {
			t.Fatal("Broadcast error:", err)
		}
	}()

	go func() {
		errChan <- mb.Close()
	}()

	for {
		select {
		case err := <-errChan:
			if err != nil {
				t.Fatal("Error when closing the broadcaster:", err)
			}
			return
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Broadcaster should have closed its producer by now")
		}
	}

}
