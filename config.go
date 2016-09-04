package orderer

import (
	"os"
	"strings"

	"github.com/Shopify/sarama"
	logging "github.com/op/go-logging"
)

// Logger ...
var Logger *logging.Logger

func init() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(backend)
	formatter := logging.MustStringFormatter("[%{time:15:04:05}] %{shortfile:18s}: %{color}[%{level:-5s}]%{color:reset} %{message}")
	logging.SetFormatter(formatter)
	Logger = logging.MustGetLogger("orderer")
}

// ConfigImpl ...
type ConfigImpl struct {
	FlagsImpl
	ConcurrentReqs int
	PartitionID    int32
	RequiredAcks   sarama.RequiredAcks
	Version        sarama.KafkaVersion
}

// FlagsImpl ...
type FlagsImpl struct {
	Brokers  []string
	LogLevel logging.Level
	Topic    string
	Port     int
	Verbose  bool
}

// NewConfig ...
func NewConfig() *ConfigImpl {
	return &ConfigImpl{
		FlagsImpl:      FlagsImpl{},
		ConcurrentReqs: 1,
		PartitionID:    0,
		RequiredAcks:   sarama.WaitForAll,
		Version:        sarama.V0_9_0_1,
	}
}

// SetLogLevel ...
func (c *ConfigImpl) SetLogLevel(level string) {
	c.LogLevel, _ = logging.LogLevel(strings.ToUpper(level)) // TODO Validate input
	logging.SetLevel(c.LogLevel, "")
}
