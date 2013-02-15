package main

import (
	"bytes"
	"flag"
	"fmt"
	"container/list"
	"os"
	es "github.com/jensrantil/rewindd/eventstore"
	zmq "github.com/alecthomas/gozmq"
)


var (
	eventStorePath = flag.String("datadir", "data",
	"directory path where incoming events will be stored. Created"+
	" if non-existent.")
	commandSocketZPath = flag.String("commandsocket",
	"tcp://127.0.0.1:9002", "Command socket. Handles new eventsand"+
	" queries.")
	eventPublishZPath = flag.String("evpubsocket",
	"tcp:127.0.0.1:9003", "ZeroMQ event publishing socket.")
)

const (
	REQUEST_DEALER_ZPATH = "inproc://distributor"
	WORKER_KILL_ZPATH = "inproc://worker-killer"
)

// Runs the server that distributes requests to workers.
// Panics on error since it is an essential piece of code required to
// run the application correctly.
func runServer(estore *es.EventStore) {
	context, err := zmq.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Close()

	commandsock, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		panic(err)
	}
	defer commandsock.Close()
	err = commandsock.Bind(*commandSocketZPath)
	if err != nil {
		panic(err)
	}

	evpubsock, err := context.NewSocket(zmq.PUB)
	if err != nil {
		panic(err)
	}
	defer evpubsock.Close()
	if binderr := evpubsock.Bind(*commandSocketZPath); binderr != nil {
		panic(binderr)
	}

	loopServer(estore, evpubsock, commandsock)
}

type zmqPollResult struct {
	nbrOfChanges int
	err error
}

// Polls a bunch of ZeroMQ sockets and notifies the result through a
// channel. This makes it possible to combine ZeroMQ polling with Go's
// own built-in channels.
func asyncPoll(notifier chan zmqPollResult, items zmq.PollItems) {
	a, b := zmq.Poll(items, -1)
	notifier <- zmqPollResult{a, b}
}

func loopServer(estore *es.EventStore, evpubsock, frontend zmq.Socket) {
	toPoll := zmq.PollItems{
		zmq.PollItem{Socket: frontend, zmq.Events: zmq.POLLIN},
	}

	pubchan := make(chan es.StoredEvent)
	estore.RegisterPublishedEventsChannel(pubchan)
	go publishAllSavedEvents(pubchan, evpubsock)

	pollchan := make(chan zmqPollResult)
	respchan := make(chan [][]byte)
	go asyncPoll(pollchan, toPoll)
	for {
		select {
		case <- pollchan:
			if toPoll[0].REvents&zmq.POLLIN != 0 {
				msg, _ := toPoll[0].Socket.RecvMultipart(0)
				go handleRequest(respchan, estore, msg)
			}
			go asyncPoll(pollchan, toPoll)
		case frames := <-respchan:
			if err := frontend.SendMultipart(frames, 0); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func publishAllSavedEvents(toPublish chan es.StoredEvent, evpub zmq.Socket) {
	msg := make([][]byte, 3)
	for {
		event := <-toPublish

		msg[0] = event.Stream
		msg[1] = event.Id
		msg[2] = event.Data

		if err := evpub.SendMultipart(msg, 0); err != nil {
			// TODO: Use logger
			fmt.Println(err)
		}
	}
}


func handleRequest(respchan chan [][]byte, estore *es.EventStore, msg [][]byte) {

	// TODO: Rename to 'framelist'
	// TODO: Avoid this creation and move it into copyList.
	parts := list.New()
	for _, msgpart := range msg {
		parts.PushBack(msgpart)
	}

	// TODO: Possibly wrap ZeroMQ router frames into a Type before
	// calling this method. That would yield a nicer API without
	// nitty gritty ZeroMQ details.
	resptemplate := list.New()
	emptyFrame := []byte("")
	for true {
		resptemplate.PushBack(parts.Remove(parts.Front()))

		if bytes.Equal(parts.Front().Value.([]byte), emptyFrame) {
			break
		}
	}

	if parts.Len() == 0 {
		errstr := "Incoming command was empty. Ignoring it."
		// TODO: Migrate to logging system
		fmt.Println(errstr)
		response := copyList(resptemplate)
		response.PushBack([]byte("ERROR " + errstr))
		respchan <- listToFrames(response)
		return
	}

	command := string(parts.Front().Value.([]byte))
	switch command {
	case "PUBLISH":
		parts.Remove(parts.Front())
		if parts.Len() != 2 {
			errstr := "Wrong number of frames for PUBLISH."
			// TODO: Migrate to logging system
			fmt.Println(errstr)
			response := copyList(resptemplate)
			response.PushBack([]byte("ERROR " + errstr))
			respchan <- listToFrames(response)
		} else {
			estream := parts.Remove(parts.Front())
			data := parts.Remove(parts.Front())
			newevent := es.UnstoredEvent{
				Stream: estream.([]byte),
				Data: data.([]byte),
			}
			estore.Add(newevent)
			// TODO: Send response
		}
	case "QUERY":
		parts.Remove(parts.Front())
		if parts.Len() != 3 {
			errstr := "Wrong number of frames for QUERY."
			// TODO: Migrate to logging system
			fmt.Println(errstr)
			response := copyList(resptemplate)
			response.PushBack([]byte("ERROR " + errstr))
			respchan <- listToFrames(response)
		} else {
			estreamprefix := parts.Remove(parts.Front())
			fromid := parts.Remove(parts.Front())
			toid := parts.Remove(parts.Front())

			events := make(chan es.StoredEvent)
			req := es.QueryRequest{
				StreamPrefix: estreamprefix.([]byte),
				FromId: fromid.([]byte),
				ToId: toid.([]byte),
			}
			go estore.Query(req, events)
			for eventdata := range(events) {
				response := copyList(resptemplate)
				response.PushBack(eventdata.Stream)
				response.PushBack(eventdata.Id)
				response.PushBack(eventdata.Data)
				// TODO: Prepend the router
				// frames before!
				respchan <- listToFrames(response)
			}
			response := copyList(resptemplate)
			response.PushBack([]byte("END"))
			respchan <- listToFrames(response)
		}
	default:
		// TODO: Move these error strings out as constants of
		//       this package.

		// TODO: Move the chunk of code below into a separate
		// function and reuse for similar piece of code above.
		errstr := "Unknown request type."
		// TODO: Migrate to logging system
		fmt.Println(errstr)
		response := copyList(resptemplate)
		response.PushBack([]byte("ERROR " + errstr))
		respchan <- listToFrames(response)
	}
}

func listToFrames(l *list.List) [][]byte {
	frames := make([][]byte, l.Len())
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		frames[i] = e.Value.([]byte)
	}
	return frames
}

func copyList(l *list.List) *list.List {
	replica := list.New()
	replica.PushBackList(l)
	return replica
}

func main() {
	flag.Parse()

	estore, err := es.NewEventStore(*eventStorePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not create event store")
		panic(err)
	}
	defer estore.Close()

	runServer(estore)

	// TODO: Handle SIGINT correctly and smoothly.
}
