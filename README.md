# kafka-orderer

An ordering service for [the new Fabric design](https://github.com/hyperledger/fabric/wiki/Next-Consensus-Architecture-Proposal) based on [Apache Kafka](http://kafka.apache.org).

![block-diagram](https://cloud.githubusercontent.com/assets/14876848/18290826/0293d9a4-7453-11e6-8d70-f89d1577c235.png)

## What's inside

The main artifact of this package is the `server` binary in `cmd/server/`. It functions as an orderer that leverages Kafka, serving the broadcast/deliver requests of the clients connected to it. A sample client is offered for testing/demo purposes, look for it in ``/cmd/client/``.

A basic sequence diagram of this thing in action (assuming a block size of 2):

![seq-diagram](https://cloud.githubusercontent.com/assets/14876848/18290347/27f5f40e-7451-11e6-8afe-eeeb67a5ce3b.png)

## Give it a test run

1. Install the Docker Engine and download the `kchristidis/kafka` image ([repo](https://github.com/kchristidis/docker-kafka))
2. Clone this repo, `cd` to it, then install the binaries: `cd cmd/server; go install; cd ../client/; go install`
3. Launch the orderer: `docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=HOST_IP_GOES_HERE --env ADVERTISED_PORT=9092 --name kafka kchristidis/kafka` (where you substitute `HOST_IP_GOES_HERE` with [the IP of your host machine](http://superuser.com/a/1080211), e.g. 192.168.0.5.)
3. Launch the orderer: `server`
4. Launch a client that broadcasts 1M messages: `client -rpc broadcast -count 1000`
5. Launch a client that requests the delivery of a stream of all blocks available on the brokers starting from block #5: `client -rpc deliver -seek -5`.

`Ctrl+C` will shutdown a process gracefully, closing the Kafka producer and consumers that were created.

## Binary flags

### server

- `brokers`: Takes a comma-separated list of Kafka brokers to connect to. Default value: `localhost:9092`
- `loglevel`: Set the logging level. Default value: `info`. (Allowed values: `info`, `debug`. [I subscribe to this logic.](http://dave.cheney.net/2015/11/05/lets-talk-about-logging))
- `period`: Maximum amount of time by which the orderer should forward a non-empty bock to Kafka. Default value: `5s`.
- `port`: The port that the gRPC sever will be listening on for incoming RPCs. Default value: `6100`.
- `size`: The maximum number of messages a block can contain. Default value: `10`
- `-verbose`: Turn of logging for the Kafka library. Default value: `false`.

### client

- `rpc`: The RPC that this client is requesting. Default value: `broadcast`. Allowed values: `broadcast`, `deliver`.
- `server`: The RPC server that this client should connect to. Default value: `localhost:6100`.
- `count`: When in broadcast mode, the number of messages to send. Default value: `100`
- `loglevel`: (See flag description in the server section above.)
- `seek`: When in deliver mode, the number of the first block that should be delivered (-2 for oldest available, -1 for newest). Default value: `2`
- `window`: When in deliver mode, how many blocks can the server send without acknowledgement. Default value: `10`.
- `ack`: When in deliver mode, send acknowledgment per this many blocks received. Default value: `7`. (Consult @jyellick's [proto file](https://github.com/kchristidis/kafka-orderer/blob/devel/ab/ab.proto) for more info on the `seek`/`window`/`ack` options.)

## TODO

A rough list, in descending priority:

- [ ] Add BDD tests
- [ ] Add option to persist/load configuration
- [ ] Validate user input
- [ ] Add TLS support
- [ ] Map proto `Status` codes to [Kafka-generated
  errors](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
- [ ] Add instrumentation
