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

package server


import (
	"testing"
	"strings"
	"math/rand"
	zmq "github.com/alecthomas/gozmq"
	"github.com/JensRantil/gorewind/eventstore"
	"github.com/syndtr/goleveldb/leveldb/storage"
	//"time"
)


func setupInMemoryeventstore() *eventstore.EventStore {
	stor := &storage.MemStorage{}
	es, err := eventstore.New(stor)
	if err != nil {
		panic(err)
	}
	return es
}

var randGen = rand.New(rand.NewSource(54))

func getRandomAlphaString(length int) string {
	chars := make([]byte, length)
	for i := range(chars) {
		chars[i] = byte('a') + byte(randGen.Intn(26))
	}
	return string(chars)
}

func getRandomInprocZMQPath() *string {
	pieces := []string{"inproc://", getRandomAlphaString(30)}
	res := strings.Join(pieces, "")
	return &res
}

func getTestServer(es *eventstore.EventStore) (*InitParams, *Server) {
	context, err := zmq.NewContext()
	if err != nil {
		panic(err)
	}
	initParams := InitParams{
		Store: es,
		CommandSocketZPath: getRandomInprocZMQPath(),
		EvPubSocketZPath: getRandomInprocZMQPath(),
		ZMQContext: context,
	}
	serv, err := New(&initParams)
	if err != nil {
		panic(err.Error())
	}
	if serv == nil {
		panic("Serv was not supposed to be nil.")
	}
	return &initParams, serv
}

func startTestServer(t *testing.T, serv *Server) {
	if err := serv.Start(); err != nil {
		t.Fatal(err)
	}
	if !serv.IsRunning() {
		t.Fatal("Expected server to be running.")
	}
}

func stopTestServer(t *testing.T, serv *Server) {
	if err := serv.Stop(); err != nil {
		t.Error("Could not stop test server.")
	}
	if err:=serv.Close(); err != nil {
		t.Error(err)
	}
}

func startStopServer(t *testing.T, serv *Server) {
	startTestServer(t, serv)
	err := serv.Stop()
	if err != nil {
		t.Error("Could not stop:", err)
	}
	if serv.IsRunning() {
		t.Error("Expected server not to be running.")
	}
}

func TestStartStop(t *testing.T) {
	t.Parallel()

	estore := setupInMemoryeventstore()
	_, serv := getTestServer(estore)
	defer serv.Close()
	startStopServer(t, serv)
}

func TestMultipleStartStop(t *testing.T) {
	t.Parallel()

	estore := setupInMemoryeventstore()
	_, serv := getTestServer(estore)
	defer serv.Close()
	for i:=0 ; i < 3 ; i++ {
		startStopServer(t, serv)
	}
}

func TestStartStart(t *testing.T) {
	t.Parallel()

	estore := setupInMemoryeventstore()
	_, serv := getTestServer(estore)
	defer serv.Close()
	if err := serv.Start(); err != nil {
		t.Error("Server could not be started.")
	}
	defer stopTestServer(t, serv)
	if err := serv.Start(); err == nil {
		t.Error("Server should not be able to start twice.")
	}
}

func TestUnknownCommand(t *testing.T) {
}

func TestPublish(t *testing.T) {
	// TODO: Assert an event was published
}

func TestMalformedPublish(t *testing.T) {
}

func TestBasicQuery(t *testing.T) {
}

func TestSlicedQuery(t *testing.T) {
}

func TestMalformedQuery(t *testing.T) {
}

func TestQueryingNonExistingEvent(t *testing.T) {
}
