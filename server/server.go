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
	"log"
	"container/list"
	"time"
	"sync"
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

	runningMutex sync.Mutex
	running bool
	stopChan chan bool
}

// IsRunning returns true if the server is running, false otherwise.
func (v *Server) IsRunning() bool {
	v.runningMutex.Lock()
	defer v.runningMutex.Unlock()
	return v.running
}

// Stop stops the server.
func (v* Server) Stop() error {
	if v.IsRunning() {
		return errors.New("Not running.")
	}

	select {
	case v.stopChan <- true:
	default:
		return errors.New("Stop already signalled.")
	}
	<-v.stopChan
	// v.running is modified by Server.Run(...)

	if v.IsRunning() {
		return errors.New("Signalled stopped, but never stopped.")
	}

	return nil
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
		running: false,
	}

	var allOkay *bool = new(bool)
	*allOkay = false
	defer func() {
		if (!*allOkay) {
			server.closeZmq()
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

func (v *Server) closeZmq() {
	(*v.evpubsock).Close()
	v.evpubsock = nil
	(*v.commandsock).Close()
	v.commandsock = nil
	(*v.context).Close()
	v.context = nil
}

func (v *Server) setRunningState(newState bool) {
	v.runningMutex.Lock()
	defer v.runningMutex.Unlock()
	v.running = newState
}

// Runs the server that distributes requests to workers.
// Panics on error since it is an essential piece of code required to
// run the application correctly.
func (v *Server) Run() {
	v.setRunningState(true)
	defer v.setRunningState(false)
	loopServer((*v).params.Store, *(*v).evpubsock, *(*v).commandsock, v.stopChan)
}

// The result of an asynchronous zmq.Poll call.
type zmqPollResult struct {
	err error
}

// Polls a bunch of ZeroMQ sockets and notifies the result through a
// channel. This makes it possible to combine ZeroMQ polling with Go's
// own built-in channels.
func asyncPoll(notifier chan zmqPollResult, items zmq.PollItems, stop chan bool) {
	for {
		timeout := time.Duration(1)*time.Second
		count, err := zmq.Poll(items, int64(timeout))
		if count > 0 || err != nil {
			notifier <- zmqPollResult{err}
		}

		select {
		case <-stop:
			stop <- true
			return
		default:
		}
	}
}

func stopPoller(cancelChan chan bool) {
	cancelChan <- true
	<-cancelChan
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
//
// TODO: Make this a type function of `Server` to remove a lot of
// parameters.
func loopServer(estore *EventStore, evpubsock, frontend zmq.Socket,
stop chan bool) {
	toPoll := zmq.PollItems{
		zmq.PollItem{Socket: frontend, zmq.Events: zmq.POLLIN},
	}

	pubchan := make(chan StoredEvent)
	estore.RegisterPublishedEventsChannel(pubchan)
	go publishAllSavedEvents(pubchan, evpubsock)

	pollchan := make(chan zmqPollResult)
	respchan := make(chan zMsg)

	pollCancel := make(chan bool)
	defer stopPoller(pollCancel)

	go asyncPoll(pollchan, toPoll, pollCancel)
	for {
		select {
		case res := <-pollchan:
			if res.err != nil {
				log.Print("Could not poll:", res.err)
			}
			if res.err == nil && toPoll[0].REvents&zmq.POLLIN != 0 {
				msg, _ := toPoll[0].Socket.RecvMultipart(0)
				zmsg := zMsg(msg)
				go handleRequest(respchan, estore, zmsg)
			}
			go asyncPoll(pollchan, toPoll, pollCancel)
		case frames := <-respchan:
			if err := frontend.SendMultipart(frames, 0); err != nil {
				log.Println(err)
			}
		case <- stop:
			stop <- true
			return
		}
	}
}

// Publishes stored events to event listeners.
//
// Pops previously stored messages off a channel and published them to a
// ZeroMQ socket.
func publishAllSavedEvents(toPublish chan StoredEvent, evpub zmq.Socket) {
	msg := make(zMsg, 3)
	for {
		event := <-toPublish

		msg[0] = event.Stream
		msg[1] = event.Id
		msg[2] = event.Data

		if err := evpub.SendMultipart(msg, 0); err != nil {
			log.Println(err)
		}
	}
}

// A single frame in a ZeroMQ message.
type zFrame []byte

// A ZeroMQ message.
//
// I wish it could have been `[]zFrame`, but that would make conversion
// from `[][]byte` pretty messy[1].
//
// [1] http://stackoverflow.com/a/15650327/260805
type zMsg [][]byte

// Handles a single ZeroMQ RES/REQ loop synchronously.
//
// The full request message stored in `msg` and the full ZeroMQ response
// is pushed to `respchan`. The function does not return any error
// because it is expected to be called asynchronously as a goroutine.
func handleRequest(respchan chan zMsg, estore *EventStore, msg zMsg) {

	// TODO: Rename to 'framelist'
	parts := list.New()
	for _, msgpart := range msg {
		parts.PushBack(msgpart)
	}

	resptemplate := list.New()
	emptyFrame := zFrame("")
	for true {
		resptemplate.PushBack(parts.Remove(parts.Front()))

		if bytes.Equal(parts.Front().Value.(zFrame), emptyFrame) {
			break
		}
	}

	if parts.Len() == 0 {
		errstr := "Incoming command was empty. Ignoring it."
		log.Println(errstr)
		response := copyList(resptemplate)
		response.PushBack(zFrame("ERROR " + errstr))
		respchan <- listToFrames(response)
		return
	}

	command := string(parts.Front().Value.(zFrame))
	switch command {
	case "PUBLISH":
		parts.Remove(parts.Front())
		if parts.Len() != 2 {
			// TODO: Constantify this error message
			errstr := "Wrong number of frames for PUBLISH."
			log.Println(errstr)
			response := copyList(resptemplate)
			response.PushBack(zFrame("ERROR " + errstr))
			respchan <- listToFrames(response)
		} else {
			estream := parts.Remove(parts.Front())
			data := parts.Remove(parts.Front())
			newevent := UnstoredEvent{
				Stream: estream.(StreamName),
				Data: data.(zFrame),
			}
			newId, err := estore.Add(newevent)
			if err != nil {
				sErr := err.Error()
				log.Println(sErr)

				response := copyList(resptemplate)
				response.PushBack(zFrame("ERROR " + sErr))
				respchan <- listToFrames(response)
			} else {
				// the event was added
				response := copyList(resptemplate)
				response.PushBack(zFrame("PUBLISHED"))
				response.PushBack(zFrame(newId))
				respchan <- listToFrames(response)
			}
		}
	case "QUERY":
		parts.Remove(parts.Front())
		if parts.Len() != 3 {
			// TODO: Constantify this error message
			errstr := "Wrong number of frames for QUERY."
			log.Println(errstr)
			response := copyList(resptemplate)
			response.PushBack(zFrame("ERROR " + errstr))
			respchan <- listToFrames(response)
		} else {
			estream := parts.Remove(parts.Front())
			fromid := parts.Remove(parts.Front())
			toid := parts.Remove(parts.Front())

			req := QueryRequest{
				Stream: estream.(zFrame),
				FromId: fromid.(zFrame),
				ToId: toid.(zFrame),
			}
			events := make(chan StoredEvent)
			// TODO: Handle errors returned below
			go estore.Query(req, events)
			for eventdata := range(events) {
				response := copyList(resptemplate)
				response.PushBack(eventdata.Stream)
				response.PushBack(eventdata.Id)
				response.PushBack(eventdata.Data)

				respchan <- listToFrames(response)
			}
			response := copyList(resptemplate)
			response.PushBack(zFrame("END"))
			respchan <- listToFrames(response)
		}
	default:
		// TODO: Move these error strings out as constants of
		//       this package.

		// TODO: Move the chunk of code below into a separate
		// function and reuse for similar piece of code above.
		// TODO: Constantify this error message
		errstr := "Unknown request type."
		log.Println(errstr)
		response := copyList(resptemplate)
		response.PushBack(zFrame("ERROR " + errstr))
		respchan <- listToFrames(response)
	}
}

// Convert a doubly linked list of message frames to a slice of message
// fram
func listToFrames(l *list.List) zMsg {
	frames := make(zMsg, l.Len())
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		frames[i] = e.Value.(zFrame)
	}
	return frames
}

// Helper function for copying a doubly linked list.
func copyList(l *list.List) *list.List {
	replica := list.New()
	replica.PushBackList(l)
	return replica
}

