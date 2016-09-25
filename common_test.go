package orderer

import (
	"testing"

	"github.com/kchristidis/kafka-orderer/ab"
)

func testClose(t *testing.T, x Closeable) {
	if err := x.Close(); err != nil {
		t.Fatal("Cannot close mock resource:", err)
	}
}

func testNewSeekMessage(startLabel string, seekNo, windowNo uint64) *ab.DeliverUpdate {
	var startVal ab.SeekInfo_Start
	switch startLabel {
	case "oldest":
		startVal = ab.SeekInfo_OLDEST
	case "newest":
		startVal = ab.SeekInfo_NEWEST
	default:
		startVal = ab.SeekInfo_SPECIFIED

	}
	return &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Seek{
			Seek: &ab.SeekInfo{
				Start:           startVal,
				SpecifiedNumber: seekNo,
				WindowSize:      windowNo,
			},
		},
	}
}

func testNewAckMessage(ackNo uint64) *ab.DeliverUpdate {
	return &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Acknowledgement{
			Acknowledgement: &ab.Acknowledgement{
				Number: ackNo,
			},
		},
	}
}
