# kafka-orderer

An ordering service for [the new Fabric design](https://github.com/hyperledger/fabric/wiki/Next-Consensus-Architecture-Proposal) based on [Apache Kafka](http://kafka.apache.org).

## Note

Development happens on the [devel](https://github.com/kchristidis/kafka-orderer/tree/devel) branch, will merge with master once a proper build is ready.

## TODO

A rough list, in descending priority:

- [ ] Implement `Deliver()` RPC
- [ ] Add RPC test client
- [ ] Add tests
- [ ] Add TLS support
- [ ] Add option to persist/load configuration
- [ ] Map proto `Status` codes to Kafka-generated errors
- [ ] Ensure consumer joins with unique group ID
- [ ] Add instrumentation
