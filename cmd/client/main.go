package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/kchristidis/kafka-orderer/ab"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type flags struct {
	rpc, server         string
	count, seek, window int
}

func main() {
	conf := flags{}
	flag.StringVar(&conf.rpc, "rpc", "broadcast", "The RPC that this client is initiating.")
	flag.StringVar(&conf.server, "server", "localhost:6100", "The gRPC server to connect to.")
	flag.IntVar(&conf.count, "count", 100, "When in broadcast mode, how many messages to send.")
	flag.IntVar(&conf.seek, "seek", 6, "When in deliver mode, the number of the first block that should be delivered (-2 for oldest available, -1 for newest).")
	flag.IntVar(&conf.window, "window", 10, "When in deliver mode, how many blocks can the server send without acknowledgement.")
	flag.Parse()

	conn, err := grpc.Dial(conf.server, grpc.WithInsecure())
	if err != nil {
		log.Fatalln("Client did not connect to %s: %v", conf.server, err)
	}
	defer conn.Close()
	c := ab.NewAtomicBroadcastClient(conn)

	switch conf.rpc {
	case "broadcast":
		broadcast(c, conf.count)
	case "deliver":
		deliver(c, conf.seek, conf.window)
	}
}

func broadcast(c ab.AtomicBroadcastClient, count int) {
	waitChan := make(chan struct{})
	stream, err := c.Broadcast(context.Background())
	if err != nil {
		log.Fatalln("Error broadcasting: %v", err)
	}

	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				close(waitChan)
				return
			}
			if err != nil {
				log.Fatalln("Failed to receive a reply: %v", err)
			}
			fmt.Fprintf(os.Stdout, "Received reply: %s\n", reply.Status.String())
		}
	}()

	message := &ab.BroadcastMessage{}
	for i := 1; i <= count; i++ {
		message.Data = []byte(strconv.Itoa(i))
		stream.Send(message)
	}

	err = stream.CloseSend()
	if err != nil {
		log.Fatalln("Error closing the broadcast stream: %v", err)
	}

	<-waitChan
}

func deliver(c ab.AtomicBroadcastClient, seek, window int) {

}
