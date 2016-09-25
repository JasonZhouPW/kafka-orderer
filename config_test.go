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
