package eventstore

import (
	"errors"
	"math/big"
	"os"
	//"code.google.com/p/leveldb-go/leveldb"
)


type EventStore struct {
	nextId *big.Int
	eventPublishers []chan StoredEvent
}


func (v *EventStore) RegisterPublishedEventsChannel(publisher chan StoredEvent) {
	v.eventPublishers = append(v.eventPublishers, publisher)
}


// TODO: This function should take an Event
func (v *EventStore) Add(event UnstoredEvent) (string, error) {
	defer v.nextId.Add(v.nextId, big.NewInt(1))
	// TODO: Implement storage
	return v.nextId.String(), nil
}


func (v* EventStore) Close() error {
	// TODO: Implement.
	return nil
}

type UnstoredEvent struct {
	Stream []byte
	Data []byte
}

type StoredEvent struct {
	Stream []byte
	Id []byte
	Data []byte
}

type QueryRequest struct {
	StreamPrefix []byte
	FromId []byte
	ToId []byte
}

// TODO: Bundle all these parameters into a type
func (v* EventStore) Query(req QueryRequest, res chan StoredEvent) {
	// TODO: Implement
	close(res)
}


// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil { return true, nil }
	if os.IsNotExist(err) { return false, nil }
	return false, err
}


// exists returns whether the given path is a directory
func isDir(path string) (bool, error) {
	fstat, err := os.Stat(path)
	if err == nil && fstat.IsDir() {
		return true, nil
	}
	return false, err
}


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


func NewEventStore(path string) (*EventStore, error) {
	if err := checkAndCreateDirPath(path); err != nil {
		return nil, err
	}
	estore := new(EventStore)
	estore.nextId = big.NewInt(0)
	// TODO: Open a database
	return new(EventStore), nil
}

