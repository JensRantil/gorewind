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
	"sync"
	"math/big"
	"strconv"
	//"code.google.com/p/leveldb-go/leveldb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/descriptor"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	iter "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	EVENT_ID_CHAN_SIZE = 100
)

// Instance of an event store. All of its functions are threadsafe.
type EventStore struct {
	eventPublishersLock sync.RWMutex
	// Using a map to avoid registering a channel multiple times
	eventPublishers map[chan StoredEvent]chan StoredEvent

	// A channel where we can make read event ID:s in a lock-free
	// way.
	eventIdChan chan string

	// Write something to this channel to quit the generator
	eventIdChanGeneratorShutdown chan bool

	db *leveldb.DB
}

// An event that has not yet been persisted to disk.
type UnstoredEvent struct {
	Stream []byte
	Data []byte
}

// An event that has previously been persisted to disk.
type StoredEvent struct {
	Stream []byte
	Id []byte
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

// Store an event to the event store. Returns the unique event id that
// the event was stored under. As long as no error occurred, of course.
func (v *EventStore) Add(event UnstoredEvent) (string, error) {
	newId := <-v.eventIdChan

	batch := new(leveldb.Batch)

	// TODO: Benchmark how much impact this write has. We could also
	// check if it exists and not write it in that case, which is
	// probably faster. Especially if we are using bloom filter.
	// TODO: Rewrite to use eventStoreKey
	streamKeyParts := [][]byte{streamPrefix, event.Stream}
	streamKey := bytes.Join(streamKeyParts, []byte(""))
	batch.Put(streamKey, []byte(""))

	evKeyParts := [][]byte{
		eventPrefix,
		event.Stream,
		[]byte(":"),
		[]byte(newId),
	}
	evKey := bytes.Join(evKeyParts, []byte(""))
	batch.Put(evKey, event.Data)

	wo := &opt.WriteOptions{}
	err := v.db.Write(batch, wo)
	if err != nil {
		return "", err
	}

	storedEvent := StoredEvent{
		Stream: event.Stream,
		Id: []byte(newId),
		Data: event.Data,
	}
	for pubchan := range v.eventPublishers {
		pubchan <- storedEvent
	}
	return newId, nil
}

// Close an open event store. A previously closed event store must never
// be used further.
func (v* EventStore) Close() error {
	v.eventIdChanGeneratorShutdown <- true
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
		req.Stream
		req.toId,
	}
	toKeyBytes = seekKey.toBytes()
	it.Seek(toKeyBytes)
	if bytes.Compare(toKeyBytes, it.Key() != 0 {
		bToId := []byte(toId)
		msg := fmt.SPrint("to key did not exist:", bToId)
		return errors.New(msg)
	}

	// From key
	seekKey := eventStoreKey{
		streamPrefix,
		req.Stream
		req.fromId,
	}
	fromKeyBytes = seekKey.toBytes()
	it.Seek(fromKeyBytes)
	if bytes.Compare(fromKeyBytes, it.Key() != 0 {
		bFromId := []byte(fromId)
		msg := fmt.SPrint("from key did not exist:", bFromId)
		return errors.New(msg)
	}

	diff := EventStreamComparer.Compare(fromKeyBytes, toKeyBytes)
	if diff >= -1 {
		msg := "The query was done in wrong chronological order."
		return errors.New(msg)
	}

	go safeQuery(it, req, res)

	return nil
}

// Make the actual query. Sanity checks of the iterator i is expected to
// have been done before calling this function.
func safeQuery(i iter.Iterator, req QueryRequest, res chan StoredEvent) error {
	if !i.Next() {
		// Querying should never return the fromKey, but be
		// inclusive when it comes to the last one. This is
		// natural, since the querier is expected to previouslye
		// have seen fromId, and the goal is to reach the state
		// of toId.
		return
	}
	for i.Next() {
		curKey := neweventStoreKey(i.Key())
		if bytes.Compare(curKey.groupKey, streamPrefix) != 0 {
			break
		}

		resEvent := {
			curKey.groupKey,
			curKey.key,
			[]byte(curKey.keyId.String()),
		}
		res <- resEvent

		if bytes.Compare(curKey.key, req.stream) != 0 {
			break
		}
		keyId := []byte(curKey.keyId.String())
		if bytes.Compare(req.toId, keyId) == 0 {
			break
		}
	}

	close(res)
}


func startEventIdGenerator(initId *string) (chan string, chan bool) {
	// TODO: Allow nextId to be set explicitly based on what's
	// previously been stored in the event store.
	nextId := big.NewInt(0)
	if initId != nil {
		nextId.SetString(*initId, 10)
		// We do not care if this succeeded. Instead, we simply
		// initialize with zero (0).
	}
	stopChan := make(chan bool)
	idChan := make(chan string, EVENT_ID_CHAN_SIZE)
	go func() {
		for {
			select {
			case idChan <- nextId.String():
				nextId.Add(nextId, big.NewInt(1))
			case <-stopChan:
				return
			}
		}
	}()
	return idChan, stopChan
}

// Create a new event store instance.
func NewEventStore(desc descriptor.Desc) (*EventStore, error) {
	estore := new(EventStore)

	// TODO: Initialize the eventid generator with maxId+1
	initId := "0"
	idChan, idChanShutdown := startEventIdGenerator(&initId)
	estore.eventIdChan = idChan
	estore.eventIdChanGeneratorShutdown = idChanShutdown

	options := &opt.Options{
		Flag: opt.OFCreateIfMissing,
		Comparer: &EventStreamComparer{},
	}
	db, err := leveldb.Open(desc, options)
	if err != nil {
		return nil, err
	}
	estore.db = db

	return estore, nil
}

// The separator used for separating into the different eventStoreKey
// fields.
var groupSep []byte = []byte(":")

// Represents a leveldb key.
type eventStoreKey struct {
	groupKey []byte
	key []byte
	keyId *big.Int
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
		pieces[2] = []byte(v.keyId.String())
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
		possibleId := big.NewInt(0)
		_, success := possibleId.SetString(string(pieces[len(pieces)-1]), 10)
		if success {
			res.keyId = possibleId
		}
	}
	if len(pieces) > 0 {
		res.groupKey = pieces[0]
	}
	if len(pieces) > 1 {
		var upperIndex int
		if res.keyId != nil {
			upperIndex = len(pieces) - 1
		} else {
			upperIndex = len(pieces)
		}
		keyPieces := pieces[1:upperIndex]
		res.key = bytes.Join(pieces, groupSep)
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
		return o1.keyId.Cmp(o2.keyId)
	case o1.keyId != nil:
		return 1
	case o2.keyId != nil:
		return -1
	default:
		return 0
	}
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

type EventStreamComparer struct {
}

func (v* EventStreamComparer) Name() string {
	return "rewindd.eventStreamComparer"
}

// If 'a' < 'b', changes 'a' to a short string in [a,b).
//
// Used to minimize the size of index blocks and other data structures.
func (v* EventStreamComparer) Separator(a, b []byte) []byte {
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
func (v* EventStreamComparer) Successor(b []byte) []byte {
	groupB := getGroup(b)
	bcomp := comparer.BytesComparer{}
	return bytes.Join([][]byte{
		bcomp.Successor(groupB),
		[]byte{},
	}, groupSep)
}

func (v* EventStreamComparer) Compare(a, b []byte) int {
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
