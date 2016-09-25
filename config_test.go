package orderer

import "time"

// TODO Probably need a better way to expose this
var (
	brokerID = int32(0)

	partitionID = int32(0)
	topic       = "test"
	config      = &ConfigImpl{
		PartitionID: partitionID,
		FlagsImpl: FlagsImpl{
			Batch: BatchConfigImpl{Period: 1000 * time.Millisecond, Size: 100},
			Topic: topic,
		},
	}

	oldestOffset = int64(100)                            // The oldest block available on the broker
	newestOffset = int64(1100)                           // The offset that will be assigned to the next block
	middleOffset = (oldestOffset + newestOffset - 1) / 2 // Just an offset in the middle
)
