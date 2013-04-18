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
	"testing/quick"
	"bytes"
	"crypto/rand"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/storage"
)


type comparatorTest struct {
	a eventStoreKey
	b eventStoreKey
	expectedResult int
}

func testComparater(t *testing.T, test comparatorTest, c eventStreamComparer) {
	testComparator(t, test, c)
	testSeparator(t, test, c)
	testSuccessor(t, test.a, c)
	testSuccessor(t, test.b, c)
}

func testSeparator(t *testing.T, test comparatorTest, c eventStreamComparer) {
	ba := test.a.toBytes()
	bb := test.b.toBytes()
	res := c.Separator(ba, bb)
	diff := c.Compare(ba, bb)
	if diff >= 0 && bytes.Compare(res, ba) != 0 {
		t.Errorf("Separator was modified when it shouldn't.")
		t.Errorf("a: %s", test.a)
		t.Errorf("b: %s", test.b)
	}
	if len(res) > len(ba) {
		t.Errorf("a was lengthened, not shortened.")
	}
	if c.Compare(ba, res) > 0 {
		t.Errorf("The Separator was less than 'a'.")
	}
	if c.Compare(bb, res) <= 0 {
		t.Errorf("The Separator was >= 'a'.")
	}
}

func testComparator(t *testing.T, test comparatorTest, comparer eventStreamComparer) {
	res := comparer.Compare(test.a.toBytes(), test.b.toBytes())
	if res != test.expectedResult {
		t.Errorf("a: %s", test.a)
		t.Errorf("b: %s", test.b)
		t.Errorf("Output was %d. Expected: %d", res,
			test.expectedResult)
	}
}

func testSuccessor(t *testing.T, s eventStoreKey, comparer eventStreamComparer) {
	bs := s.toBytes()
	shorter := comparer.Successor(bs)
	if len(shorter) > len(bs) {
		t.Errorf("Successor was longer: %s", s)
	}
	if comparer.Compare(shorter, bs) < 0 {
		t.Errorf("Successor was greater the its origin: %s", s)
	}
}

func TestComparator(t *testing.T) {
	t.Parallel()

	tests := []comparatorTest{
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				nil,
			},
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				nil,
			},
			0,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				nil,
			},
			eventStoreKey{
				[]byte("g"),
				[]byte("b"),
				nil,
			},
			-1,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				nil,
			},
			eventStoreKey{
				[]byte("h"),
				[]byte("a"),
				nil,
			},
			-1,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				newByteCounter(),
			},
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				newByteCounter(),
			},
			0,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				newByteCounter(),
			},
			eventStoreKey{
				[]byte("h"),
				[]byte("a"),
				newByteCounter(),
			},
			-1,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				newByteCounter().toBytes(),
			},
			eventStoreKey{
				[]byte("g"),
				[]byte("a"),
				newByteCounter().NewIncrementedCounter(),
			},
			-1,
		},
		comparatorTest{
			eventStoreKey{
				[]byte("g"),
				[]byte("a:b"),
				newByteCounter(),
			},
			eventStoreKey{
				[]byte("g"),
				[]byte("a:b"),
				newByteCounter().NewIncrementedCounter(),
			},
			-1,
		},
	}

	comparer := eventStreamComparer{}
	for _, test := range(tests) {
		testComparator(t, test, comparer)
		if test.expectedResult != 0 {
			// Testing the inverted case
			invertedTest := comparatorTest{
				test.b,
				test.a,
				-test.expectedResult,
			}
			testComparator(t, invertedTest, comparer)
		}

		equalityTest := comparatorTest{
			test.a,
			test.a,
			0,
		}
		testComparator(t, equalityTest, comparer)

		equalityTest = comparatorTest{
			test.b,
			test.b,
			0,
		}
		testComparator(t, equalityTest, comparer)

	}
}

type evStoreKeySerTest struct {
	origin string
	group string
	key string
	keyId byteCounter
}

func TestEventStoreKeySerialization(t *testing.T) {
	t.Parallel()

	f := func(groupId, key, bKeyId []byte) bool {
		// GroupId must not contain any :
		groupId = bytes.Replace(groupId, groupSep, []byte(""), -1)
		if bytes.Contains(groupId, groupSep) {
			t.Log("It still contained separator:", groupSep)
			t.Log("Bytes:", groupId)
			return false
		}

		ev := eventStoreKey{
			groupId,
			key,
			loadByteCounter(bKeyId),
		}

		parsed, err := newEventStoreKey(ev.toBytes())
		if err != nil {
			return false
		}
		return parsed.Compare(&ev) == 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error("QuickTest failed:", err)
	}
}

func TestReverseBytes(t *testing.T) {
	t.Parallel()

	a := []byte{0, 255}
	b := []byte{255, 0}
	reverseBytes(a)
	if bytes.Compare(a, b) != 0 {
		t.Error("Not equal:", a, b)
	}

	a = []byte{0, 128, 255}
	b = []byte{255, 128, 0}
	reverseBytes(a)
	if bytes.Compare(a, b) != 0 {
		t.Error("Not equal:", a, b)
	}

	f := func(a []byte) bool {
		b := make([]byte, len(a))
		copy(b, a)
		reverseBytes(b)
		reverseBytes(b)
		return bytes.Compare(a, b)==0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error("QuickTest failed:", err)
	}
}

func TestByteCounter(t *testing.T) {
	t.Parallel()

	a := []byte{255}
	a = wrapBytes(a)
	if len(a) <= 1 {
		t.Error("Incorrect wrap:", a)
	}

	counter := newByteCounter()
	for i:= 0 ; i < 5000 ; i++ {
		if counter.Compare(counter) != 0 {
			t.Error("Not equal to itself.")
		}

		reloaded := loadByteCounter(counter.toBytes())
		if reloaded.Compare(counter) != 0 {
			t.Error("Reloaded should be equal to itself")
		}

		nextC := counter.NewIncrementedCounter()
		if nextC.Compare(counter) < 0 {
			t.Error("Increment not greater:")
			t.Error(" * Original: ", counter)
			t.Error(" * Increment:", nextC)
		}
		if counter.Compare(nextC) > 0 {
			t.Error("Original not smaller.:")
			t.Error(" * Original: ", counter)
			t.Error(" * Increment:", nextC)
		}

		counter = nextC
	}
}

type byteSorter [][]byte
func (b byteSorter) Len() int {
	return len(b)
}
func (b byteSorter) Less(i, j int) bool {
	return bytes.Compare(b[i], b[j]) < 0
}
func (b byteSorter) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func TestIdGenerator(t *testing.T) {
	t.Parallel()

	gen := newStreamIdGenerator()

	n := 10
	ids := make(byteSorter, 0, n)
	for i:=0 ; i < n ; i++ {
		id, err := gen.Allocate([]byte("mystream"))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	sort.Sort(ids)
	for i:=1 ; i < n ; i++ {
		if bytes.Compare(ids[i-1], ids[i])==0 {
			t.Fatal(i, "Found duplicate id:", ids[i])
		}
	}
}

func TestAtomicByteCounter(t *testing.T) {
	t.Parallel()

	gen := newAtomicbyteCounter(newByteCounter())

	n := 10
	ids := make(byteSorter, 0, n)
	for i:=0 ; i < n ; i++ {
		id := gen.Next()
		ids = append(ids, id)
	}
	sort.Sort(ids)
	for i:=1 ; i < n ; i++ {
		if bytes.Compare(ids[i-1], ids[i])==0 {
			t.Fatal(i, "Found duplicate id:", ids[i])
		}
	}
}

func setupInMemoryeventstore() (es* EventStore, err error) {
	stor := &storage.MemStorage{}
	es, err = NewEventStore(stor)
	return
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

	es, err := setupInMemoryeventstore()
	if err != nil {
		t.Fatal(err)
	}

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
		testEvent := UnstoredEvent{
			Stream: stream,
			Data: randBytes(10),
		}
		id, err := es.Add(testEvent)
		for _, ev := range(events) {
			if bytes.Compare(ev.Id, id)==0 {
				t.Fatal("ID duplicate found.")
			}
		}

		events = append(events, StoredEvent{
			Stream: stream,
			Id: id,
			Data: testEvent.Data,
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

	es, err := setupInMemoryeventstore()
	if err != nil {
		t.Fatal(err)
	}
	stream := []byte("mystream")
	testAddAndQuery(t, es, stream, 1)
}

func TestMultipleAddAndQuery(t *testing.T) {
	t.Parallel()

	es, err := setupInMemoryeventstore()
	if err != nil {
		t.Fatal(err)
	}
	stream := []byte("mystream")
	testAddAndQuery(t, es, stream, 2)
}

func TestMultipleStreamMultiAddAndQuery(t *testing.T) {
	t.Parallel()

	es, err := setupInMemoryeventstore()
	if err != nil {
		t.Fatal(err)
	}
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

	es, err := setupInMemoryeventstore()
	if err != nil {
		t.Fatal(err)
	}
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
