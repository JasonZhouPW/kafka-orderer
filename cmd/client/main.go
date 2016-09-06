package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"

	"github.com/kchristidis/kafka-orderer/ab"
	logging "github.com/op/go-logging"
	"google.golang.org/grpc"
)

var logger *logging.Logger

type configImpl struct {
	logLevel                 logging.Level
	rpc, server              string
	count, seek, window, ack int
}

type clientImpl struct {
	config     configImpl
	rpc        ab.AtomicBroadcastClient
	signalChan chan os.Signal
}

func main() {
	var loglevel string
	client := &clientImpl{}

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(backend)
	formatter := logging.MustStringFormatter("[%{time:15:04:05}] %{shortfile:18s}: %{color}[%{level:-5s}]%{color:reset} %{message}")
	logging.SetFormatter(formatter)
	logger = logging.MustGetLogger("orderer/client")

	flag.StringVar(&client.config.rpc, "rpc", "broadcast",
		"The RPC that this client is requesting.")
	flag.StringVar(&client.config.server, "server",
		"localhost:6100", "The RPC server to connect to.")
	flag.IntVar(&client.config.count, "count", 100,
		"When in broadcast mode, how many messages to send.")
	flag.StringVar(&loglevel, "loglevel", "info",
		"The logging level. (Allowed values: info, debug)")
	flag.IntVar(&client.config.seek, "seek", 2,
		"When in deliver mode, the number of the first block that should be delivered (-2 for oldest available, -1 for newest).")
	flag.IntVar(&client.config.window, "window", 10,
		"When in deliver mode, how many blocks can the server send without acknowledgement.")
	flag.IntVar(&client.config.ack, "ack", 7,
		"When in deliver mode, send acknowledgment per this many blocks received.")
	flag.Parse() // TODO Validate user input (e.g. ack should be =< window)

	client.config.logLevel, _ = logging.LogLevel(strings.ToUpper(loglevel))
	logging.SetLevel(client.config.logLevel, "orderer/client")

	// Trap SIGINT to trigger a shutdown
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	client.signalChan = make(chan os.Signal, 1)
	signal.Notify(client.signalChan, os.Interrupt)

	conn, err := grpc.Dial(client.config.server, grpc.WithInsecure())
	if err != nil {
		logger.Fatalf("Client did not connect to %s: %v\n", client.config.server, err)
	}
	defer conn.Close()
	client.rpc = ab.NewAtomicBroadcastClient(conn)

	switch client.config.rpc {
	case "broadcast":
		client.broadcast()
	case "deliver":
		client.deliver()
	}
}
