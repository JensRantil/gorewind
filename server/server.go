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

// Contains the server loop. Deals with incoming requests and delegates
// them to the event store.
package server

import (
	"bytes"
	"errors"
	"fmt"
	"container/list"
	zmq "github.com/alecthomas/gozmq"
)

type InitParams struct {
	Store *EventStore
	CommandSocketZPath *string
	EvPubSocketZPath *string
}

// Check all required initialization parameters are set.
func checkAllInitParamsSet(p *InitParams) error {
	if p.Store == nil {
		return errors.New("Missing param: Store")
	}
	if p.CommandSocketZPath == nil {
		return errors.New("Missing param: CommandSocketZPath")
	}
	if p.EvPubSocketZPath == nil {
		return errors.New("Missing param: EvPubSocketZPath")
	}
	return nil
}

// A server instance. Can be run.
type Server struct {
	params InitParams

	evpubsock *zmq.Socket
	commandsock *zmq.Socket
	context *zmq.Context
}

// Initialize a new event store server and return a handle to it. The
// event store is not started. It's up to the caller to execute Run()
// on the server handle.
func New(params *InitParams) (*Server, error) {
	if params == nil {
		return nil, errors.New("Missing init params")
	}
	if err := checkAllInitParamsSet(params); err != nil {
		return nil, err
	}

	server := Server{
		params: *params,
	}

	var allOkay *bool = new(bool)
	*allOkay = false
	defer func() {
		if (!*allOkay) {
			server.Close()
		}
	}()

	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	server.context = &context

	commandsock, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	server.commandsock = &commandsock
	err = commandsock.Bind(*params.CommandSocketZPath)
	if err != nil {
		return nil, err
	}

	evpubsock, err := context.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	server.evpubsock = &evpubsock
	if binderr := evpubsock.Bind(*params.EvPubSocketZPath); binderr != nil {
		return nil, err
	}

	*allOkay = true

	return &server, nil
}

func (v *Server) Close() {
	(*v.evpubsock).Close()
	v.evpubsock = nil
	(*v.commandsock).Close()
	v.commandsock = nil
	(*v.context).Close()
	v.context = nil
}

// Runs the server that distributes requests to workers.
// Panics on error since it is an essential piece of code required to
// run the application correctly.
func (v *Server) Run() {
	loopServer((*v).params.Store, *(*v).evpubsock, *(*v).commandsock)
}

// The result of an asynchronous zmq.Poll call.
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

// The core ZeroMQ messaging loop. Handles requests and responses
// asynchronously using the router socket. Every request is delegated to
// a goroutine for maximum concurrency.
//
// `gozmq` does currently not support copy-free messages/frames. This
// means that every message passing through this function needs to be
// copied in-memory. If this becomes a bottleneck in the future,
// multiple router sockets can be hooked to this final router to scale
// message copying.
func loopServer(estore *EventStore, evpubsock, frontend zmq.Socket) {
	toPoll := zmq.PollItems{
		zmq.PollItem{Socket: frontend, zmq.Events: zmq.POLLIN},
	}

	pubchan := make(chan StoredEvent)
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

// Publishes stored events to event listeners.
//
// Pops previously stored messages off a channel and published them to a
// ZeroMQ socket.
func publishAllSavedEvents(toPublish chan StoredEvent, evpub zmq.Socket) {
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

// Handles a single ZeroMQ RES/REQ loop.
//
// The full request message stored in `msg` and the full ZeroMQ response
// is pushed to `respchan`. The function does not return any error
// because it is expected to be called asynchronously as a goroutine.
func handleRequest(respchan chan [][]byte, estore *EventStore, msg [][]byte) {

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
			newevent := UnstoredEvent{
				Stream: estream.([]byte),
				Data: data.([]byte),
			}
			newId, err := estore.Add(newevent)
			if err != nil {
				// TODO: Migrate to logging system
				sErr := err.Error()
				fmt.Println(sErr)

				response := copyList(resptemplate)
				response.PushBack([]byte("ERROR " + sErr))
				respchan <- listToFrames(response)
			} else {
				// the event was added
				response := copyList(resptemplate)
				response.PushBack([]byte("PUBLISHED"))
				response.PushBack([]byte(newId))
				respchan <- listToFrames(response)
			}
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

			events := make(chan StoredEvent)
			req := QueryRequest{
				Stream: estreamprefix.([]byte),
				FromId: fromid.([]byte),
				ToId: toid.([]byte),
			}
			go estore.Query(req, events)
			for eventdata := range(events) {
				response := copyList(resptemplate)
				response.PushBack(eventdata.Stream)
				response.PushBack(eventdata.Id)
				response.PushBack(eventdata.Data)

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

// Convert a doubly linked list of message frames to a slice of message
// fram
func listToFrames(l *list.List) [][]byte {
	frames := make([][]byte, l.Len())
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		frames[i] = e.Value.([]byte)
	}
	return frames
}

// Helper function for copying a doubly linked list.
func copyList(l *list.List) *list.List {
	replica := list.New()
	replica.PushBackList(l)
	return replica
}

