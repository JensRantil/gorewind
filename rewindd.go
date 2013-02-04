package main

import (
	"github.com/jensrantil/rewindd/eventstore"
	"flag"
	"fmt"
	"os"
)


var (
	eventStorePath = flag.String("datadir", "data",
	"directory path where incoming events will be stored. Created"+
	" if non-existent.")
)


func loop(estore *eventstore.EventStore) {
	
}


func main() {
	flag.Parse()

	estore, err := eventstore.NewEventStore(*eventStorePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not create event store")
		panic(err)
	}

	loop(estore)
}
