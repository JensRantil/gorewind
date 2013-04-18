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
	"sort"
)

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
