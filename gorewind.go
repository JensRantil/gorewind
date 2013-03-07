package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/jensrantil/gorewind/server"
	"github.com/syndtr/goleveldb/leveldb/descriptor"
)


var (
	eventStorePath = flag.String("datadir", "data",
	"directory path where incoming events will be stored. Created"+
	" if non-existent.")
	commandSocketZPath = flag.String("commandsocket",
	"tcp://127.0.0.1:9002", "Command socket. Handles new eventsand"+
	" queries.")
	eventPublishZPath = flag.String("evpubsocket",
	"tcp://127.0.0.1:9003", "ZeroMQ event publishing socket.")
)

// Main method. Will panic if things are so bad that the application
// will not start.
func main() {
	flag.Parse()

	desc, err := descriptor.OpenFile(*eventStorePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not create descriptor")
		panic(err)
	}
	defer desc.Close()

	estore, err := server.NewEventStore(desc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not create event store")
		panic(err)
	}
	defer estore.Close()

	initParams := server.InitParams{
		Store: estore,
	}
	serv, err := server.New(&initParams)
	if err == nil {
		panic(err.Error())
	}

	// TODO: Handle SIGINT correctly and smoothly.

	serv.Run()
}
