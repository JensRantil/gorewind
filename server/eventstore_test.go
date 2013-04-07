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
	f := func(groupId, key, bKeyId []byte) bool {
		// GroupId must not contain any :
		bytes.Replace(groupId, []byte(":"), []byte(""), -1)

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

// TODO: Test byteCounter
