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
	"bytes"
	"errors"
	"sync"
)

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
func loadByteCounter(bs []byte) byteCounter {
	return bs
}

// Reverse a byte slice.
func reverseBytes(b []byte) {
	rIndex := len(b) - 1
	for lIndex := range b[0:(len(b)/2)] {
		b[rIndex], b[lIndex] = b[lIndex], b[rIndex]
		rIndex -= 1
	}
}

// Helper function used when incrementing a byteCounter.
func wrapBytes(bs []byte) []byte {
	for i, el := range bs {
		if el < 255 {
			bs[i] += 1
			return bs
		} else {
			// el == 255
			bs[i] = 0
		}
	}
	bs = append(bs, 1)
	return bs
}

// Create a brand new incremented byteCounter based on a previous one.
func (v byteCounter) NewIncrementedCounter() (incr byteCounter) {
	incr = make([]byte, len(v), len(v) + 1)
	copy(incr, v)

	reverseBytes(incr)
	incr = wrapBytes(incr)
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

type atomicbyteCounter struct {
	nextUnused byteCounter
	lock sync.Mutex
}

func newAtomicbyteCounter(next byteCounter) (c *atomicbyteCounter) {
	c = new(atomicbyteCounter)
	c.nextUnused = next
	return
}

func (c *atomicbyteCounter) Next() byteCounter {
	c.lock.Lock()
	defer c.lock.Unlock()

	res := c.nextUnused
	c.nextUnused = c.nextUnused.NewIncrementedCounter()

	return res
}

// Keeps track of ordered set of byteCounters, one per registered stream.
type streamIdGenerator struct {
	// key type must be string because []byte is not a valid key
	// data type.
	counters map[string]*atomicbyteCounter

	// lock for counters
	lock sync.RWMutex
}

func newStreamIdGenerator() (s *streamIdGenerator) {
	s = new(streamIdGenerator)
	s.counters = make(map[string]*atomicbyteCounter)
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
		// TODO: Constantify this error
		return errors.New("name already registered")
	}

	g.lock.Lock()
	defer g.lock.Unlock()
	g.counters[string(name)] = newAtomicbyteCounter(init)

	return nil
}

// Allocate a new unused counter for a specific stream.
//
// TODO: Remove error return value. It never happens.
func (g *streamIdGenerator) Allocate(name StreamName) (byteCounter, error) {
	if !g.isRegistered(name) {
		// Auto registering
		g.Register(name, []byte{0})
	}

	g.lock.RLock()
	defer g.lock.RUnlock()
	counter := g.counters[string(name)]
	res := counter.Next()
	return res, nil
}
