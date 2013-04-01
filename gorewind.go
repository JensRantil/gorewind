// gorewind is an event store server written in Python that talks ZeroMQ.
// Copyright (C) 2013  Jens Rantil
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// GoRewind is an event store server written in Go that talks ZeroMQ.
package main

import (
	"flag"
	"log"
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

	log.Println("Event store to use:", *eventStorePath)
	log.Println("Command socket path:", *commandSocketZPath)
	log.Println("Event publishing socket path:", *eventPublishZPath)
	log.Println()

	desc, err := descriptor.OpenFile(*eventStorePath)
	if err != nil {
		log.Panicln("could not create DB descriptor")
	}
	defer desc.Close()

	estore, err := server.NewEventStore(desc)
	if err != nil {
		log.Panicln(os.Stderr, "could not create event store")
	}
	defer estore.Close()

	initParams := server.InitParams{
		Store: estore,
		CommandSocketZPath: commandSocketZPath,
		EvPubSocketZPath: eventPublishZPath,
	}
	serv, err := server.New(&initParams)
	if err != nil {
		panic(err.Error())
	}

	// FIXME: Handle SIGINT correctly and smoothly.

	serv.Run()
}
