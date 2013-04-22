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
package eventstore

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
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
func New(stor storage.Storage) (*EventStore, error) {
	estore := new(EventStore)

	ePublishers := make(map[chan StoredEvent]chan StoredEvent)
	estore.eventPublishers = ePublishers

	options := &opt.Options{
		Flag: opt.OFCreateIfMissing,
		Comparer: &eventStreamComparer{},
	}
	db, err := leveldb.Open(stor, options)
	if err != nil {
		return nil, err
	}
	estore.db = db

	estore.idGenerator, err = initStreamIdGenerator(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	return estore, nil
}

// Helper function to initialize a streamIdGenerator.
func initStreamIdGenerator(db *leveldb.DB) (*streamIdGenerator, error) {
	gen := newStreamIdGenerator()

	searchKey := eventStoreKey{
		streamPrefix,
		nil,
		nil,
	}

	ro := &opt.ReadOptions{}
	it := db.NewIterator(ro)
	it.Seek(searchKey.toBytes())
	for it.Valid() {
		key, err := newEventStoreKey(it.Key())
		if err != nil {
			log.Println("A key could not be deserialized:")
			log.Println(string(it.Key()))
			return nil, err
		}
		if bytes.Compare(key.groupKey, streamPrefix) != 0 {
			// We have reached the end of the stream listing
			break
		}

		stream := key.key
		latestId := loadByteCounter(it.Value())
		nextId := latestId.NewIncrementedCounter()
		err = gen.Register(stream, nextId)
		if err != nil {
			return nil, err
		}

		it.Next()
	}

	return gen, nil
}

// An event that has not yet been persisted to disk.
type Event struct {
	// The name of the stream to which this event shall be stored.
	Stream StreamName

	// The data that is to be stored for this event. Can be an
	// arbitrary byte slice.
	Data []byte
}

// An event that has previously been persisted to disk.
type StoredEvent struct {
	// The ID for the stored event. No other event exists with name
	// Stream and ID Id.
	Id EventId

	// The event that was persisted.
	Event
}

// Register a channel where are published events will be pushed to.
// Multiple channels can be registered.
func (v *EventStore) RegisterPublishedEventsChannel(publisher chan StoredEvent) {
	v.eventPublishersLock.Lock()
	defer v.eventPublishersLock.Unlock()
	v.eventPublishers[publisher] = publisher
}

var streamPrefix []byte = []byte("stream")
var eventPrefix []byte = []byte("event")

type EventId []byte

// Store an event to the event store. Returns the unique event id that
// the event was stored under. As long as no error occurred, of course.
func (v *EventStore) Add(event Event) (EventId, error) {
	newId, err := v.idGenerator.Allocate(event.Stream)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)

	// TODO: Benchmark how much impact this write has. We could also
	// check if it exists and not write it in that case, which is
	// probably faster. Especially if we are using bloom filter.
	streamKey := eventStoreKey{
		streamPrefix,
		event.Stream,
		nil,
	}
	batch.Put(streamKey.toBytes(), newId.toBytes())

	evKey := eventStoreKey{
		eventPrefix,
		event.Stream,
		newId.toBytes(),
	}
	batch.Put(evKey.toBytes(), event.Data)

	wo := &opt.WriteOptions{}
	err = v.db.Write(batch, wo)
	if err != nil {
		return nil, err
	}

	storedEvent := StoredEvent{
		[]byte(newId),
		event,
	}

	v.eventPublishersLock.RLock()
	defer v.eventPublishersLock.RUnlock()
	for pubchan := range v.eventPublishers {
		pubchan <- storedEvent
	}

	return EventId(newId), nil
}

// List the available streams through a stream.
func (v* EventStore) ListStreams(start StreamName, maxItems int) chan StreamName {
	res := make(chan StreamName)
	go func() {
		defer close(res)
		ro := &opt.ReadOptions{}
		it := v.db.NewIterator(ro)

		seekKey := eventStoreKey{
			streamPrefix,
			start,
			nil,
		}
		it.Seek(seekKey.toBytes())

		i := 0
		for it.Valid() && i < maxItems {
			key, err := newEventStoreKey(it.Key())
			if err != nil {
				// This should never happen
				panic(err)
			}
			if bytes.Compare(key.groupKey, streamPrefix) != 0 {
				break
			}
			res <- key.key
			it.Next()
			i++
		}
	}()
	return res
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
// asynchronously.
//
// TODO: Also make the error checking asynchronously, to
// minimize IO blocking when calling this function.
func (v *EventStore) Query(req QueryRequest) (chan StoredEvent, error) {
	ro := &opt.ReadOptions{}
	it := v.db.NewIterator(ro)

	// To key
	var toKeyBytes []byte
	if req.ToId != nil {
		seekKey := eventStoreKey{
			streamPrefix,
			req.Stream,
			loadByteCounter(req.ToId),
		}
		toKeyBytes = seekKey.toBytes()
		it.Seek(toKeyBytes)
		if bytes.Compare(toKeyBytes, it.Key()) != 0 {
			bToId := string(toKeyBytes)
			msg := fmt.Sprint("to key did not exist:", bToId)
			return nil, errors.New(msg)
		}
	}

	// From key
	var fromKeyBytes []byte
	if req.FromId != nil {
		seekKey := eventStoreKey{
			eventPrefix,
			req.Stream,
			loadByteCounter(req.FromId),
		}
		fromKeyBytes = seekKey.toBytes()
		it.Seek(fromKeyBytes)
		if bytes.Compare(fromKeyBytes, it.Key()) != 0 {
			bFromId := string(fromKeyBytes)
			msg := fmt.Sprint("from key did not exist:", bFromId)
			return nil, errors.New(msg)
		}
	} else {
		seekKey := eventStoreKey{
			eventPrefix,
			req.Stream,
			newByteCounter(),
		}
		fromKeyBytes = seekKey.toBytes()
		it.Seek(fromKeyBytes)
	}

	if req.FromId != nil && req.ToId != nil {
		diff := new(eventStreamComparer).Compare(fromKeyBytes, toKeyBytes)
		if diff >= -1 {
			msg := "The query was done in wrong chronological order."
			return nil, errors.New(msg)
		}
	}

	res := make(chan StoredEvent)
	go safeQuery(it, req, res)

	return res, nil
}

// Make the actual query. Sanity checks of the iterator i is expected to
// have been done before calling this function.
func safeQuery(i iter.Iterator, req QueryRequest, res chan StoredEvent) {
	defer close(res)
	for i.Valid() {
		curKey, err := newEventStoreKey(i.Key())
		if err != nil {
			log.Println("A key could not be deserialized:")
			// Panicing here, because this error most
			// certainly needs to be looked at by a
			// an operator.
			log.Panicln(string(i.Key()))
		}

		if bytes.Compare(curKey.groupKey, eventPrefix) != 0 {
			break
		}
		if bytes.Compare(curKey.key, req.Stream) != 0 {
			break
		}

		resEvent := StoredEvent{
			curKey.keyId.toBytes(),
			Event{
				curKey.key,
				[]byte(i.Value()),
			},
		}
		res <- resEvent

		keyId := curKey.keyId.toBytes()
		if req.ToId != nil && bytes.Compare(req.ToId, keyId) == 0 {
			break
		}

		i.Next()
	}
}

// A stream name.
type StreamName []byte
