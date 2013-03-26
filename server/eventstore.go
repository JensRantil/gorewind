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

// Deals with persisting events to disk and querying them. No network is
// involved in any of the code in this package.
package server

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"strconv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/descriptor"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	iter "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Instance of an event store. All of its functions are threadsafe.
type EventStore struct {
	eventPublishersLock sync.RWMutex
	// Using a map to avoid registering a channel multiple times
	eventPublishers map[chan StoredEvent]chan StoredEvent

	idGenerator *streamIdGenerator

	db *leveldb.DB
}

// Create a new event store instance.
func NewEventStore(desc descriptor.Desc) (*EventStore, error) {
	estore := new(EventStore)

	ePublishers := make(map[chan StoredEvent]chan StoredEvent)
	estore.eventPublishers = ePublishers

	options := &opt.Options{
		Flag: opt.OFCreateIfMissing,
		Comparer: &eventStreamComparer{},
	}
	db, err := leveldb.Open(desc, options)
	if err != nil {
		return nil, err
	}
	estore.db = db

	estore.idGenerator = initStreamIdGenerator(db)

	return estore, nil
}

// Helper function to initialize a streamIdGenerator.
func initStreamIdGenerator(db *leveldb.DB) (gen *streamIdGenerator) {
	gen = new(streamIdGenerator)

	// TODO: Initialize the streamIdGenerator from database

	return
}

// An event that has not yet been persisted to disk.
type UnstoredEvent struct {
	// The name of the stream to which this event shall be stored.
	Stream StreamName
	// The data that is to be stored for this event. Can be an
	// arbitrary byte slice.
	Data []byte
}

// An event that has previously been persisted to disk.
type StoredEvent struct {
	// The name of the stream in which this event is stored.
	Stream StreamName
	// The ID for the stored event. No other event exists with name
	// Stream and ID Id.
	Id EventId
	// The data stored for the event. Can be an arbitrary byte
	// slice.
	Data []byte
}

// Register a channel where are published events will be pushed to.
// Multiple channels can be registered.
func (v *EventStore) RegisterPublishedEventsChannel(publisher chan StoredEvent) {
	// TODO: Implement an UnregisterPublishedEventsChannel.
	v.eventPublishersLock.Lock()
	defer v.eventPublishersLock.Unlock()
	v.eventPublishers[publisher] = publisher
}

var streamPrefix []byte = []byte("stream")
var eventPrefix []byte = []byte("event")

// An incrementable byte slice. It's orderable and is used to have
// strict ordering between events within a single event stream.
type byteCounter []byte

// Create a brand new byteCounter and initialize it to 0.
func newByteCounter() byteCounter {
	bs := make([]byte, 1)
	bs[0] = 0
	return bs
}

// Load a byteCounter from a byte slice.
func loadbyteCounter(bs []byte) byteCounter {
	return bs
}

// Reverse a byte slice.
func reverseBytes(b []byte) {
	stop := len(b) / 2
	rIndex := len(b) - 1
	for lIndex, _ := range b {
		if lIndex >= stop {
			return
		}
		rIndex -= 1
		// TODO: Benchmark if using range element below would
		// make an improvement in speed.
		b[rIndex], b[lIndex] = b[lIndex], b[rIndex]
	}
}

// Helper function used when incrementing a byteCounter.
func wrapBytes(bs []byte) {
	for i, el := range bs {
		if el != 255 {
			bs[i] += 1
			return
		} else {
			bs[i] = 0
			if i == len(bs) - 1 {
				bs = append(bs, 1)
				return
			}
		}
	}
}

// Create a brand new incremented byteCounter based on a previous one.
func (v *byteCounter) NewIncrementedCounter() (incr byteCounter) {
	incr = make([]byte, len(*v), len(*v) + 1)
	copy(incr, *v)

	reverseBytes(incr)
	incr[0] += 1
	if incr[0] == 0 {
		// if we wrapped
		wrapBytes(incr[1:])
	}
	reverseBytes(incr)
	return
}

// Compare byteCounter a with byteCounter b. Returns -1 if a is smaller
// than b, 0 if they are equal, 1 if b is smaller than a.
func (a *byteCounter) Compare(b byteCounter) int {
	if len(*a) < len(b) {
		return -1
	}
	if len(*a) > len(b) {
		return 1
	}
	return bytes.Compare(*a, b)
}

// Convert a byteCounter to a byte slice. Ie., serialize it.
func (a byteCounter) toBytes() []byte {
	return a
}

type EventId []byte

// Store an event to the event store. Returns the unique event id that
// the event was stored under. As long as no error occurred, of course.
func (v *EventStore) Add(event UnstoredEvent) (EventId, error) {
	newId, err := v.idGenerator.Allocate(event.Stream)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)

	// TODO: Benchmark how much impact this write has. We could also
	// check if it exists and not write it in that case, which is
	// probably faster. Especially if we are using bloom filter.
	// TODO: Rewrite to use eventStoreKey
	streamKeyParts := [][]byte{streamPrefix, event.Stream}
	streamKey := bytes.Join(streamKeyParts, []byte(""))
	batch.Put(streamKey, newByteCounter().toBytes())

	evKeyParts := [][]byte{
		eventPrefix,
		event.Stream,
		[]byte(":"),
		[]byte(newId),
	}
	evKey := bytes.Join(evKeyParts, []byte(""))
	batch.Put(evKey, event.Data)

	wo := &opt.WriteOptions{}
	err = v.db.Write(batch, wo)
	if err != nil {
		return nil, err
	}

	storedEvent := StoredEvent{
		Stream: event.Stream,
		Id: []byte(newId),
		Data: event.Data,
	}

	v.eventPublishersLock.RLock()
	defer v.eventPublishersLock.RUnlock()
	for pubchan := range v.eventPublishers {
		pubchan <- storedEvent
	}

	return EventId(newId), nil
}

// Close an open event store. A previously closed event store must never
// be used further.
func (v* EventStore) Close() error {
	return nil
}

// A query request.
type QueryRequest struct {
	Stream []byte
	FromId []byte
	ToId []byte
}

// Query events from an event store. If the request is malformed in any
// way, an error is returned. Otherwise, the query result is streamed
// through the res channel in chronological order.
//
// Currently this function will make error checks synchronously.  If all
// looks good, streaming the results through `res` is done
// asynchronously. TODO: Also make the error checking asynchronously, to
// minimize IO blocking when calling this function.
func (v *EventStore) Query(req QueryRequest, res chan StoredEvent) error {
	ro := &opt.ReadOptions{}
	it := v.db.NewIterator(ro)

	// To key
	seekKey := eventStoreKey{
		streamPrefix,
		req.Stream,
		loadbyteCounter(req.ToId),
	}
	toKeyBytes := seekKey.toBytes()
	it.Seek(toKeyBytes)
	if bytes.Compare(toKeyBytes, it.Key()) != 0 {
		bToId := string(toKeyBytes)
		msg := fmt.Sprint("to key did not exist:", bToId)
		return errors.New(msg)
	}

	// From key
	seekKey = eventStoreKey{
		streamPrefix,
		req.Stream,
		loadbyteCounter(req.FromId),
	}
	fromKeyBytes := seekKey.toBytes()
	it.Seek(fromKeyBytes)
	if bytes.Compare(fromKeyBytes, it.Key()) != 0 {
		bFromId := string(fromKeyBytes)
		msg := fmt.Sprint("from key did not exist:", bFromId)
		return errors.New(msg)
	}

	diff := new(eventStreamComparer).Compare(fromKeyBytes, toKeyBytes)
	if diff >= -1 {
		msg := "The query was done in wrong chronological order."
		return errors.New(msg)
	}

	go safeQuery(it, req, res)

	return nil
}

// Make the actual query. Sanity checks of the iterator i is expected to
// have been done before calling this function.
func safeQuery(i iter.Iterator, req QueryRequest, res chan StoredEvent) {
	defer close(res)
	for i.Next() {
		curKey := neweventStoreKey(i.Key())
		if bytes.Compare(curKey.groupKey, streamPrefix) != 0 {
			break
		}

		resEvent := StoredEvent{
			curKey.groupKey,
			curKey.key,
			curKey.keyId.toBytes(),
		}
		res <- resEvent

		if bytes.Compare(curKey.key, req.Stream) != 0 {
			break
		}
		keyId := curKey.keyId.toBytes()
		if bytes.Compare(req.ToId, keyId) == 0 {
			break
		}
	}
}

type atomicbyteCounter struct {
	nextUnused byteCounter
	lock sync.Mutex
}

func newAtomicbyteCounter(next byteCounter) (c *atomicbyteCounter) {
	c = new(atomicbyteCounter)
	c.nextUnused = next
	return
}

func (c *atomicbyteCounter) Next() (res byteCounter) {
	c.lock.Lock()
	defer c.lock.Unlock()

	res = c.nextUnused
	c.nextUnused = c.nextUnused.NewIncrementedCounter()

	return
}

// A stream name.
type StreamName []byte

// Keeps track of ordered set of byteCounters, one per registered stream.
type streamIdGenerator struct {
	// key type must be string because []byte is not a valid key
	// data type.
	counters map[string]atomicbyteCounter

	// lock for counters
	lock sync.RWMutex
}

func newStreamIdGenerator() (s *streamIdGenerator) {
	s = new(streamIdGenerator)
	s.counters = make(map[string]atomicbyteCounter)
	return
}

// Check whether a named counter previously has been registered.
func (g *streamIdGenerator) isRegistered(name StreamName) (exists bool) {
	g.lock.RLock()
	defer g.lock.RUnlock()
	_, exists = g.counters[string(name)]
	return
}

// Register a new byteCounter.
func (g *streamIdGenerator) Register(name StreamName, init byteCounter) error {
	if g.isRegistered(name) {
		return errors.New("name already registered")
	}

	g.lock.Lock()
	defer g.lock.Unlock()
	g.counters[string(name)] = *newAtomicbyteCounter(init)
	return nil
}

// Allocate a new unused counter for a specific stream.
func (g *streamIdGenerator) Allocate(name StreamName) (byteCounter, error) {
	if !g.isRegistered(name) {
		// Auto registering
		g.Register(name, []byte{0})
	}

	g.lock.Lock()
	defer g.lock.Unlock()
	counter := g.counters[string(name)]
	return counter.Next(), nil
}


// The separator used for separating into the different eventStoreKey
// fields.
var groupSep []byte = []byte(":")

// Represents a leveldb key.
type eventStoreKey struct {
	groupKey []byte
	key []byte
	keyId byteCounter
}

// Convert a eventStoreKey to bytes. The returned byte slice is either
// "groupKey:key:keyId" if keyId is non-nil, or "groupKey:key"
// otherwise.
func (v *eventStoreKey) toBytes() []byte {
	var pieces [][]byte
	if v.keyId != nil {
		pieces = make([][]byte, 3)
		pieces[0] = v.groupKey
		pieces[1] = v.key
		pieces[2] = v.keyId
	} else {
		pieces = make([][]byte, 2)
		pieces[0] = v.groupKey
		pieces[1] = v.key
	}
	return bytes.Join(pieces, groupSep)

}

// Convert a byte slice to a parsed eventStoreKey.
func neweventStoreKey(data []byte) (*eventStoreKey) {
	res := new(eventStoreKey)
	pieces := bytes.Split(data, groupSep)
	if len(pieces) > 2 {
		res.keyId = pieces[len(pieces)-1]
	}
	if len(pieces) > 0 {
		res.groupKey = pieces[0]
	}
	if len(pieces) > 1 {
		var upperIndex int
		// TODO: Handle the case when len(pieces)>=max(int)
		if res.keyId != nil {
			upperIndex = len(pieces) - 1
		} else {
			upperIndex = len(pieces)
		}
		keyPieces := pieces[1:upperIndex]
		res.key = bytes.Join(keyPieces, groupSep)
	}
	return res
}

// Compare to another eventStoreKey. Returns -1 if this one is smaller
// than o2, 0 same, or 1 is this one is bigger than the previous one.
func (o1 *eventStoreKey) compare(o2 *eventStoreKey) int {
	if diff := bytes.Compare(o1.groupKey, o2.groupKey); diff != 0 {
		return diff
	}
	if diff := bytes.Compare(o1.key, o2.key); diff != 0 {
		return diff
	}
	switch {
	case o1.keyId != nil && o2.keyId != nil:
		return o1.keyId.Compare(o2.keyId)
	case o1.keyId != nil:
		return 1
	case o2.keyId != nil:
		return -1
	}
	return 0
}

// Helper functions for comparer

func getGroup(key []byte) []byte {
	return bytes.SplitN(key, groupSep, 1)[0]
}

func getRealKey(key []byte) []byte {
	pieces := bytes.Split(key, groupSep)
	if _, err := getIntegerPart(key); err != nil {
		return bytes.Join(pieces[1:len(pieces)], groupSep)
	}
	return bytes.Join(pieces[1:len(pieces) - 1], groupSep)
}

func getIntegerPart(key []byte) (int, error) {
	pieces := bytes.Split(key, groupSep)
	lastpiece := pieces[len(pieces) - 1]
	i, err := strconv.Atoi(string(lastpiece))
	if err != nil {
		return 0, err
	}
	return i, nil
}

// Comparer

type eventStreamComparer struct {
}

func (v* eventStreamComparer) Name() string {
	return "rewindd.eventStreamComparer"
}

// If 'a' < 'b', changes 'a' to a short string in [a,b).
//
// Used to minimize the size of index blocks and other data structures.
func (v* eventStreamComparer) Separator(a, b []byte) []byte {
	groupA := getGroup(a)
	groupB := getGroup(b)
	if c := bytes.Compare(groupA, groupB); c != 0 {
		bcomp := comparer.BytesComparer{}
		return bytes.Join([][]byte{
			bcomp.Separator(groupA, groupB),
			[]byte{},
		}, groupSep)
	}
	// Here we know that groupA==groupB

	realKeyA := getRealKey(a)
	realKeyB := getRealKey(b)
	if c := bytes.Compare(realKeyA, realKeyB); c != 0 {
		bcomp := comparer.BytesComparer{}
		return bytes.Join([][]byte{
			groupA,
			bcomp.Separator(realKeyA, realKeyA),
		}, groupSep)
	}
	// Here we know that realKeyA==realKeyB

	// TODO: Handle this
	intPartA, errA := getIntegerPart(a)
	intPartB, errB := getIntegerPart(b)
	switch {
	case errA == nil && errB == nil:
		// [Group, key, intA] </>/= [Group, key, intB]
		switch {
		case intPartA < intPartB:
			return bytes.Join([][]byte{
				groupA,
				realKeyA,
				[]byte(strconv.Itoa(intPartB - 1)),
			}, groupSep)
		/*case intPartA > intPartB:
			return a*/
		default:
			return a
		}
	case errA != nil && errB != nil:
		// [Group, key] == [Group, key]
		return a
	case errA != nil:
		// [Group, key, int] > [Group, key]
		return a
	}
	//default: -- must be put outside of switch to avoid compiler
	//error.
	// [Group, key] < [Group, key, int]
	return bytes.Join([][]byte{
		groupA,
		realKeyA,
		[]byte("1"),
	}, groupSep)

	// Unoptimized result that always works.
	return a
}

// Changes 'b' to a short string >= 'b'
//
// Used to minimize the size of index blocks and other data structures.
func (v* eventStreamComparer) Successor(b []byte) []byte {
	groupB := getGroup(b)
	bcomp := comparer.BytesComparer{}
	return bytes.Join([][]byte{
		bcomp.Successor(groupB),
		[]byte{},
	}, groupSep)
}

func (v* eventStreamComparer) Compare(a, b []byte) int {
	groupA := getGroup(a)
	groupB := getGroup(b)
	if c := bytes.Compare(groupA, groupB); c != 0 {
		return c
	}

	realKeyA := getRealKey(a)
	realKeyB := getRealKey(b)
	if c := bytes.Compare(realKeyA, realKeyB); c != 0 {
		return c
	}

	intPartA, errA := getIntegerPart(a)
	intPartB, errB := getIntegerPart(b)
	switch {
	case errA == nil && errB == nil:
		// [Group, key, intA] </>/= [Group, key, intB]
		switch {
		case intPartA < intPartB:
			return -1
		case intPartA > intPartB:
			return 1
		default:
			return 0
		}
	case errA != nil && errB != nil:
		// [Group, key] == [Group, key]
		return 0
	case errA != nil:
		// [Group, key, int] > [Group, key]
		return 1
	}
	//default: -- must be put outside of switch to avoid compiler
	//error.
	// [Group, key] < [Group, key, int]
	return -1
}
