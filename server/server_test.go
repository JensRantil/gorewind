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
//	zmq "github.com/alecthomas/gozmq"
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
	initParams := InitParams{
		Store: es,
		CommandSocketZPath: getRandomInprocZMQPath(),
		EvPubSocketZPath: getRandomInprocZMQPath(),
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

func startStopServer(t *testing.T, serv *Server) {
	serv.Start()
	//time.Sleep(1*time.Second/1000)
	if !serv.IsRunning() {
		t.Error("Expected server to be running.")
	}
	err := serv.Stop()
	if err != nil {
		t.Error("Could not stop:", err)
	}
	if serv.IsRunning() {
		t.Error("Expected server not to be running.")
	}
}

func TestStartStop(t *testing.T) {
	estore := setupInMemoryeventstore()
	_, serv := getTestServer(estore)
	startStopServer(t, serv)
}

func TestMultipleStartStop(t *testing.T) {
	estore := setupInMemoryeventstore()
	_, serv := getTestServer(estore)
	for i:=0 ; i < 3 ; i++ {
		startStopServer(t, serv)
	}
}
