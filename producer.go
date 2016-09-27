/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package orderer

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kchristidis/kafka-orderer/config"
)

// Producer allows the caller to send blocks to the orderer
type Producer interface {
	Send(payload []byte) error
	Closeable
}

type producerImpl struct {
	producer sarama.SyncProducer
	topic    string
}

func newProducer(conf *config.TopLevel) Producer {
	brokerConfig := newBrokerConfig(conf)
	p, err := sarama.NewSyncProducer(conf.Kafka.Brokers, brokerConfig)
	if err != nil {
		panic(fmt.Errorf("Failed to create Kafka producer: %v", err))
	}
	return &producerImpl{producer: p, topic: conf.Kafka.Topic}
}

func (p *producerImpl) Close() error {
	return p.producer.Close()
}

func (p *producerImpl) Send(payload []byte) error {
	_, offset, err := p.producer.SendMessage(newMsg(payload, p.topic))
	if err == nil {
		Logger.Debugf("Forwarded block %v to ordering service\n", offset)
	}
	return err
}
