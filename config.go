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
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	logging "github.com/op/go-logging"
)

// Logger is the package-level logging object
var Logger *logging.Logger

func init() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(backend)
	formatter := logging.MustStringFormatter("[%{time:15:04:05}] %{shortfile:18s}: %{color}[%{level:-5s}]%{color:reset} %{message}")
	logging.SetFormatter(formatter)
	Logger = logging.MustGetLogger("orderer")
	logging.SetLevel(logging.INFO, "")
}

// ConfigImpl contains all configuration options needed for a Kafka orderer
type ConfigImpl struct {
	FlagsImpl
	ConcurrentReqs int
	PartitionID    int32
	RequiredAcks   sarama.RequiredAcks
	Version        sarama.KafkaVersion
}

// FlagsImpl contains the options that can be set by the caller via command-line flags
type FlagsImpl struct {
	Batch    BatchConfigImpl
	Brokers  []string
	LogLevel logging.Level
	Topic    string
	Port     int
	Verbose  bool
}

// BatchConfigImpl contains the configurations options related to message batching
type BatchConfigImpl struct {
	Period time.Duration
	Size   int
}

// NewConfig returns an orderer config object with the proper partition ID for the Kafka broker
func NewConfig() *ConfigImpl {
	return &ConfigImpl{
		// ConcurrentReqs: 1,
		PartitionID: 0,
		// RequiredAcks:   sarama.WaitForAll,
		Version: sarama.V0_9_0_1,
	}
}

// SetLogLevel sets the package log level
func (c *ConfigImpl) SetLogLevel(level string) {
	c.LogLevel, _ = logging.LogLevel(strings.ToUpper(level)) // TODO Validate input
	logging.SetLevel(c.LogLevel, "")
}
