// Deals with persisting events to disk and querying them. No network is
// involved in any of the code in this package.
package eventstore

import (
	"errors"
	"sync"
	"math/big"
	"os"
	//"code.google.com/p/leveldb-go/leveldb"
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

// A query request.
type QueryRequest struct {
	StreamPrefix []byte
	FromId []byte
	ToId []byte
}

// Register a channel where are published events will be pushed to.
// Multiple channels can be registered.
func (v *EventStore) RegisterPublishedEventsChannel(publisher chan StoredEvent) {
	// TODO: Implement an UnregisterPublishedEventsChannel.
	v.eventPublishersLock.Lock()
	defer v.eventPublishersLock.Unlock()
	v.eventPublishers[publisher] = publisher
}

// Store an event to the event store. Returns the unique event id that
// the event was stored under. As long as no error occurred, of course.
func (v *EventStore) Add(event UnstoredEvent) (string, error) {
	newId := <-v.eventIdChan
	// TODO: Write to storage backend
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

// Query events from an event store. If the request is malformed in any
// way, an error is returned. Otherwise, the query result is streamed
// through the res channel in chronological order.
func (v* EventStore) Query(req QueryRequest, res chan StoredEvent) error {
	// TODO: Implement
	close(res)
	return nil
}


// Checks whether the given file or directory exists or not.
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil { return true, nil }
	if os.IsNotExist(err) { return false, nil }
	return false, err
}


// Checks whether the given path is a directory or not.
func isDir(path string) (bool, error) {
	fstat, err := os.Stat(path)
	if err == nil && fstat.IsDir() {
		return true, nil
	}
	return false, err
}

// Create a directory if it does not exist.
func checkAndCreateDirPath(path string) error {
	exists, err := exists(path)
	if err != nil {
		return err
	}
	if !exists {
		os.MkdirAll(path, 0700)
	}

	// deliberately checking if directory was created correctly
	// here. Might as well...
	isdir, err := isDir(path)
	if err != nil {
		return err
	}
	if !isdir {
		return errors.New("the event store path is not a directory")
	}
	return nil
}

func startEventIdGenerator() (chan string, chan bool) {
	// TODO: Allow nextId to be set explicitly based on what's
	// previously been stored in the event store.
	nextId := big.NewInt(0)
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
func NewEventStore(path string) (*EventStore, error) {
	if err := checkAndCreateDirPath(path); err != nil {
		return nil, err
	}
	estore := new(EventStore)
	estore.eventIdChan, estore.eventIdChanGeneratorShutdown = startEventIdGenerator()
	// TODO: Open a database
	return new(EventStore), nil
}

