package orderer

import "testing"

func TestProducer(t *testing.T) {
	mp := mockNewProducer(t, config, middleOffset, make(chan []byte))
	defer testClose(t, mp)
}
