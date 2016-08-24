package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/kchristidis/kafka-orderer/ab"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type flags struct {
	rpc, server              string
	count, seek, window, ack int
}

func main() {
	conf := flags{}
	flag.StringVar(&conf.rpc, "rpc", "broadcast", "The RPC that this client is initiating.")
	flag.StringVar(&conf.server, "server", "localhost:6100", "The gRPC server to connect to.")
	flag.IntVar(&conf.count, "count", 100, "When in broadcast mode, how many messages to send.")
	flag.IntVar(&conf.seek, "seek", 6, "When in deliver mode, the number of the first block that should be delivered (-2 for oldest available, -1 for newest).")
	flag.IntVar(&conf.window, "window", 10, "When in deliver mode, how many blocks can the server send without acknowledgement.")
	flag.IntVar(&conf.ack, "ack", 10, "When in deliver mode, send acknowledgment for every [ack] blocks received.")
	flag.Parse()

	conn, err := grpc.Dial(conf.server, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Client did not connect to %s: %v\n", conf.server, err)
	}
	defer conn.Close()
	c := ab.NewAtomicBroadcastClient(conn)

	switch conf.rpc {
	case "broadcast":
		broadcast(c, conf.count)
	case "deliver":
		deliver(c, conf.seek, conf.window, conf.ack)
	}
}

func broadcast(c ab.AtomicBroadcastClient, count int) {
	waitChan := make(chan struct{})
	stream, err := c.Broadcast(context.Background())
	if err != nil {
		log.Fatalln("Error broadcasting: ", err)
	}

	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				close(waitChan)
				return
			}
			if err != nil {
				log.Fatalln("Failed to receive a reply: ", err)
			}
			fmt.Fprintf(os.Stdout, "Broadcast reply from gRPC server: %s\n", reply.Status.String())
		}
	}()

	message := &ab.BroadcastMessage{}
	for i := 1; i <= count; i++ {
		message.Data = []byte(strconv.Itoa(i))
		err := stream.Send(message)
		if err != nil {
			log.Println("Error sending broadcast message: ", err)
		}
	}

	err = stream.CloseSend()
	if err != nil {
		log.Fatalln("Error closing the broadcast stream: ", err)
	}

	<-waitChan
}

func deliver(c ab.AtomicBroadcastClient, seek, window, ack int) {
	var count int
	var startFrom ab.SeekInfo_Start
	var startNumber uint64
	updateAck := &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Acknowledgement{
			Acknowledgement: &ab.Acknowledgement{
				Number: uint64(0),
			},
		},
	}

	switch seek {
	case -2:
		startFrom = ab.SeekInfo_OLDEST
	case -1:
		startFrom = ab.SeekInfo_NEWEST
	default:
		startFrom = ab.SeekInfo_SPECIFIED
		startNumber = uint64(seek)
	}
	waitChan := make(chan struct{})
	stream, err := c.Deliver(context.Background())
	if err != nil {
		log.Fatalln("Error delivering: ", err)
	}

	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				close(waitChan)
				return
			}
			if err != nil {
				log.Fatalln("Failed to receive a deliver reply: ", err)
			}
			switch t := reply.GetType().(type) {
			case *ab.DeliverReply_Block:
				fmt.Fprintf(os.Stdout, "Deliver reply from gRPC server (block): %+v\n", t.Block)
				count++
				if (count > 0) && (count%ack == 0) {
					updateAck.GetAcknowledgement().Number = t.Block.Number
					err = stream.Send(updateAck)
					if err != nil {
						log.Println("Error sending ACK update: ", err)
					}
				}
			case *ab.DeliverReply_Error:
				fmt.Fprintf(os.Stdout, "Deliver reply from gRPC server (error): %+v\n", t.Error.String())
			}
		}
	}()

	updateSeek := &ab.DeliverUpdate{
		Type: &ab.DeliverUpdate_Seek{
			Seek: &ab.SeekInfo{
				Start:           startFrom,
				SpecifiedNumber: startNumber,
				WindowSize:      uint64(window),
			},
		},
	}
	err = stream.Send(updateSeek)
	if err != nil {
		log.Println("Error sending seek update: ", err)
	}

	// Trap SIGINT to trigger a shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	for {
		select {
		case <-signalChan:
			err = stream.CloseSend()
			if err != nil {
				log.Fatalln("Error closing the deliver stream: ", err)
			}
			return
		case <-waitChan:
			return
		}
	}
}
