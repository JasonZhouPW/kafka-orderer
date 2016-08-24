# kafka-orderer

An ordering service for [the new Fabric design](https://github.com/hyperledger/fabric/wiki/Next-Consensus-Architecture-Proposal) based on [Apache Kafka](http://kafka.apache.org).

## Note

Development happens on the [devel](https://github.com/kchristidis/kafka-orderer/tree/devel) branch, will merge with master once a proper build is ready.

## Instructions

1. Clone this repo, `cd` to it, then install the binaries: `cd cmd/server; go install; cd ../client/; go install`
2. Launch the orderer: `docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=HOST_IP_GOES_HERE --env ADVERTISED_PORT=9092 --name kafka kchristidis/kafka` (where you substitute `HOST_IP_GOES_HERE` with [the IP of your host machine](http://superuser.com/a/1080211), e.g. 192.168.0.5.)
3. Launch the gRPC server: `server`
4. Have the gRPC client broadcast 100 messages: `client -rpc broadcast -count 100`
5. Use the gRPC client to deliver these messages: `client -rpc deliver -seek -2 -window 10 -ack 5`. This will deliver all the messages available on the broker (`-seek 2`: seek the oldest block available, `-seek 1`: start from the next available block, `-seek x` where `x >= 0`: start delivering from block `x`), with a window of 10 blocks. Acknowledgements will be sent to the orderer for every 5 blocks received. Consult @jyellick's [proto file](https://github.com/kchristidis/kafka-orderer/blob/devel/ab/ab.proto) for more info.

For simplicity each block carries a single message.

## TODO

A rough list, in descending priority:

- [x] Implement `Deliver()` RPC
- [x] Add RPC test client
- [ ] Add tests
- [ ] Add TLS support
- [ ] Add option to persist/load configuration
- [ ] Map proto `Status` codes to [Kafka-generated
  errors](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
- [ ] Ensure consumer joins with unique group ID
- [ ] Add instrumentation
