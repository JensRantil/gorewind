package eventstore

import (
	"bytes"
	"log"
	"github.com/syndtr/goleveldb/leveldb/comparer"
)

type eventStreamComparer struct {
}

func (v* eventStreamComparer) Name() string {
	return "rewindd.eventStreamComparer"
}

// If 'a' < 'b', changes 'a' to a short string in [a,b).
//
// Used to minimize the size of index blocks and other data structures.
func (v* eventStreamComparer) Separator(a, b []byte) []byte {
	keyA, err := newEventStoreKey(a)
	if err != nil {
		log.Println("A key could not be deserialized:")
		log.Println(string(a))

		// Always a safe result
		return a
	}
	keyB, err := newEventStoreKey(b)
	if err != nil {
		log.Println("A key could not be deserialized:")
		log.Println(string(b))

		// Always a safe result
		return a
	}

	if c := bytes.Compare(keyA.groupKey, keyB.groupKey); c != 0 {
		bcomp := comparer.BytesComparer{}
		smallerKey := eventStoreKey{
			bcomp.Separator(keyA.groupKey, keyB.groupKey),
			nil,
			nil,
		}
		return smallerKey.toBytes()
	}
	// Here we know that keyA.groupKey==keyB.groupKey

	if c := bytes.Compare(keyA.key, keyB.key); c != 0 {
		bcomp := comparer.BytesComparer{}
		smallerKey := eventStoreKey{
			keyA.groupKey,
			bcomp.Separator(keyA.key, keyB.key),
			nil,
		}
		return smallerKey.toBytes()
	}
	// Here we know that keyA.key==keyB.key

	// Unoptimized result that always works.
	// Can't make it shorter than it already is :-/
	return a
}

// Changes 'b' to a short string >= 'b'
//
// Used to minimize the size of index blocks and other data structures.
func (v* eventStreamComparer) Successor(b []byte) []byte {
	keyB, err := newEventStoreKey(b)
	if err != nil {
		log.Println("A key could not be deserialized:")
		log.Println(string(b))

		// Always a safe result
		return b
	}
	bcomp := comparer.BytesComparer{}
	successor := eventStoreKey{
		bcomp.Successor(keyB.groupKey),
		nil,
		nil,
	}
	return successor.toBytes()
}

func (v* eventStreamComparer) Compare(a, b []byte) int {
	keyA, err := newEventStoreKey(a)
	if err != nil {
		log.Println("A key could not be deserialized:")
		// Unrecoverable error here. This should never happen!
		log.Panicln(string(a))
	}
	keyB, err := newEventStoreKey(b)
	if err != nil {
		log.Println("A key could not be deserialized:")
		// Unrecoverable error here. This should never happen!
		log.Panicln(string(b))
	}
	return keyA.Compare(keyB)
}
