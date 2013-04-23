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

package eventstore


import (
	"testing"
	"bytes"
	"crypto/rand"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/storage"
)


func setupInMemoryeventstore() *EventStore {
	stor := &storage.MemStorage{}
	es, err := New(stor)
	if err != nil {
		// so that calling test does not have to deal with this.
		panic(err)
	}
	return es
}

func popAllEvents(c chan StoredEvent, t *testing.T) (es []StoredEvent) {
	es = make([]StoredEvent, 0, 100)
	for e := range c {
		es = append(es, e)
	}
	return
}

func TestQueryingEmptyStream(t *testing.T) {
	t.Parallel()

	es := setupInMemoryeventstore()

	count := 0
	streams := es.ListStreams(nil, 30)
	for _ = range(streams) {
		count++
	}
	if count > 0 {
		t.Error("Did not expect any event stream to exist.")
	}

	res, err := es.Query(QueryRequest{Stream: []byte("mystream")})
	if err != nil {
		t.Fatal(err)
	}
	events := popAllEvents(res, t)
	if len(events) != 0 {
		t.Error("Wrong number of events:")
		t.Error("Expected: 0")
		t.Error("Was:     ", len(events), events)
	}
}

func randBytes(n int) []byte {
	res := make([]byte, n)
	rand.Reader.Read(res)
	return res
}

func testAddAndQuery(t *testing.T, es *EventStore, stream StreamName, n int) {
	events := make([]StoredEvent, 0, n)
	for i := 0 ; i < n ; i++ {
		testEvent := Event{
			stream,
			randBytes(10),
		}
		id, err := es.Add(testEvent)
		for _, ev := range(events) {
			if bytes.Compare(ev.Id, id)==0 {
				t.Fatal("ID duplicate found.")
			}
		}

		events = append(events, StoredEvent{
			id,
			Event{
				stream,
				testEvent.Data,
			},
		})

		count := 0
		streams := es.ListStreams(nil, 30)
		for s := range(streams) {
			if bytes.Compare(s, stream) == 0 {
				count++
			}
		}
		if count != 1 {
			t.Error("Expected the stream to always exist.")
		}

		res, err := es.Query(QueryRequest{Stream: stream})
		if err != nil {
			t.Fatal(err)
		}
		count = 0
		for e := range(res) {
			if bytes.Compare(events[count].Stream,
			e.Stream) != 0 {
				t.Error(count, "Stream not matching:")
				t.Error("Expected:",
				events[count].Stream)
				t.Error("Was:", e.Stream)
			}
			if bytes.Compare(events[count].Id,
			e.Id) != 0 {
				t.Error(count, "Id not matching:")
				t.Error("Expected:", events[count].Id)
				t.Error("Was:", e.Id)
			}
			if bytes.Compare(events[count].Data, e.Data) != 0 {
				t.Error(count, "Data not matching:")
				t.Error("Expected:",
				events[count].Data)
				t.Error("Was:", e.Data)
			}
			count++
		}
		if len(events) != count {
			t.Error("Wrong number of events:")
			t.Error("Expected:", count)
			t.Error("Was:     ", len(events))
		}
	}
}

func TestSingleAddAndQuery(t *testing.T) {
	t.Parallel()

	es := setupInMemoryeventstore()
	stream := []byte("mystream")
	testAddAndQuery(t, es, stream, 1)
}

func TestMultipleAddAndQuery(t *testing.T) {
	t.Parallel()

	es := setupInMemoryeventstore()
	stream := []byte("mystream")
	testAddAndQuery(t, es, stream, 2)
}

func TestMultipleStreamMultiAddAndQuery(t *testing.T) {
	t.Parallel()

	es := setupInMemoryeventstore()
	streams := []StreamName{
		StreamName("stream1"),
		StreamName("stream2"),
		StreamName("stream4"),
		StreamName("stream11"),
	}
	for _, stream := range(streams) {
		testAddAndQuery(t, es, stream, 5)
	}
}

func testConcurrentAddAndQuery(t *testing.T) {
	t.Parallel()

	es := setupInMemoryeventstore()
	streams := []StreamName{
		StreamName("stream1"),
		StreamName("stream2"),
		StreamName("stream4"),
		StreamName("stream11"),
	}

	wgroup := sync.WaitGroup{}
	for _, stream := range(streams) {
		wgroup.Add(1)
		go func() {
			testAddAndQuery(t, es, stream, 100)
			wgroup.Done()
		}()
	}
	wgroup.Wait()
}
