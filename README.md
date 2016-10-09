# kafka-orderer

An ordering service for [the new Fabric design](https://github.com/hyperledger/fabric/wiki/Next-Consensus-Architecture-Proposal) based on [Apache Kafka](http://kafka.apache.org).

![block-diagram](https://cloud.githubusercontent.com/assets/14876848/18880485/f8d45098-84a5-11e6-83f0-1dc419b26d9b.png)

## What's inside

The main artifact of this package is the `server` binary in `cmd/server/`. It functions as an orderer that leverages Kafka, serving the broadcast/deliver requests of the clients connected to it. A sample client is offered for testing/demo purposes, look for it in ``/cmd/bd_counter/``.

A basic sequence diagram of this thing in action (assuming a block size of 2):

![seq-diagram](https://cloud.githubusercontent.com/assets/14876848/18290347/27f5f40e-7451-11e6-8afe-eeeb67a5ce3b.png)

## Give it a test run

1. Install the Docker Engine and download the `kchristidis/kafka` image ([repo](https://github.com/kchristidis/docker-kafka))
2. Clone this repo, `cd` to it, `git checkout devel`, then install the binaries: `$ cd cmd/server; go install; cd ../bd_counter/; go install`
3. Launch the orderer: `$ docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=0.0.0.0 --env ADVERTISED_PORT=9092 --name kafka kchristidis/kafka`
3. Launch the orderer: `$ server`
4. Launch a client that broadcasts 1000 messages: `$ bd_counter -rpc broadcast -count 1000`
5. Launch a client that requests the delivery of a stream of all blocks available on the brokers starting from block #5: `$ bd_counter -rpc deliver -seek 5`.

`Ctrl+C` will shutdown a process gracefully, closing the Kafka producer and consumers that were created.

## Binary flags

### server

- `loglevel`: Set the logging level for the `orderer` library. Default value: `info`. (Allowed values: `info`, `debug`. [I subscribe to this logic.](http://dave.cheney.net/2015/11/05/lets-talk-about-logging))
- `-verbose`: Turn of logging for the Kafka library. Default value: `false`.

### bd_counter

- `rpc`: The RPC that this client is requesting. Default value: `broadcast`. Allowed values: `broadcast`, `deliver`.
- `server`: The RPC server that this client should connect to. Default value: `127.0.0.1:5151`.
- `count`: When in broadcast mode, the number of messages to send. Default value: `100`
- `loglevel`: (See flag description in the server section above.)
- `seek`: When in deliver mode, the number of the first block that should be delivered (-2 for oldest available, -1 for newest). Default value: `-2`
- `window`: When in deliver mode, how many blocks can the server send without acknowledgement. Default value: `10`.
- `ack`: When in deliver mode, send acknowledgment per this many blocks received. Default value: `7`. (Consult the [proto file](https://github.com/kchristidis/kafka-orderer/blob/devel/ab/ab.proto) for more info on the `seek`/`window`/`ack` options.)

## Docker Compose

If you want to build the `orderer` image from scratch:

`CGO_ENABLED=0 GOOS=linux go build -a -o cmd/server/server github.com/kchristidis/kafka-orderer/cmd/server; docker build -t kchristidis/orderer .; docker-compose up`

Otherwise simply do:

`docker-compose up`

This will bring up a server that can be reached at the default port on your Docker host.

## TODO

A rough list, in descending priority:

- [ ] Validate user input
- [ ] Map proto `Status` codes to [Kafka-generated
  errors](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
- [ ] Add instrumentation
- [ ] Add support for multiple orderers
- [ ] Support TLS
