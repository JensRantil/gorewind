package eventstore

import (
	"errors"
	"math/big"
	"os"
	//"code.google.com/p/leveldb-go/leveldb"
)


type EventStore struct {
	nextid *big.Int
}


func (v *EventStore) Add(data []byte) string {
	defer v.nextid.Add(v.nextid, big.NewInt(1))
	return v.nextid.String()
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
	estore.nextid = big.NewInt(0)
	// TODO: Open a database
	return new(EventStore), nil
}

