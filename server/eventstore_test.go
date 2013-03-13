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
	a string
	b string
	expectedResult int
}

func testComparater(t *testing.T, test comparatorTest, c EventStreamComparer) {
	testComparator(t, test, c)
	testSeparator(t, test, c)
	testSuccessor(t, test.a, c)
	testSuccessor(t, test.b, c)
}

func testSeparator(t *testing.T, test comparatorTest, c EventStreamComparer) {
	ba := []byte(test.a)
	bb := []byte(test.b)
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

func testComparator(t *testing.T, test comparatorTest, comparer EventStreamComparer) {
	res := comparer.Compare([]byte(test.a), []byte(test.b))
	if res != test.expectedResult {
		t.Errorf("a: %s", test.a)
		t.Errorf("b: %s", test.b)
		t.Errorf("Output was %d. Expected: %d", res,
			test.expectedResult)
	}
}

func testSuccessor(t *testing.T, s string, comparer EventStreamComparer) {
	bs := []byte(s)
	shorter := comparer.Successor(bs)
	if len(shorter) > len(bs) {
		t.Errorf("Successor was longer: %s", s)
	}
	if comparer.Compare(shorter, bs) < 0 {
		t.Errorf("Successor was greater the its origin: %s", s)
	}
}

func TestComparator(t *testing.T) {
	comparer := EventStreamComparer{}
	tests := []comparatorTest{
		comparatorTest{"g:a", "g:a", 0},
		comparatorTest{"g:a", "g:b", -1},
		comparatorTest{"g:a", "h:a", -1},
		comparatorTest{"g:a:1", "g:a:1", 0},
		comparatorTest{"g:a:1", "h:a:1", -1},
		comparatorTest{"g:a:1", "h:a:2", -1},
		comparatorTest{"g:a:1", "h:a:11", -1},
		comparatorTest{"g:a:b:1", "g:a:b:11", -1},

		// Test cases where there are multiple colon keys in the
		// middle.
		comparatorTest{"g:a:b", "g:a:b", 0},
		comparatorTest{"g:a:b", "g:b:c", -1},
		comparatorTest{"g:a:b", "h:a:b", -1},
		comparatorTest{"g:a:b:1", "g:a:b:1", 0},
		comparatorTest{"g:a:q:1", "h:a:q:1", -1},
		comparatorTest{"g:a:b:1", "h:a:b:2", -1},
		comparatorTest{"g:a:b:1", "h:a:b:11", -1},
	}
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
	}
}

type evStoreKeySerTest struct {
	origin string,
	group string,
	key string,
	keyId *big.Int,
}

func TestEventStoreKeySerialization(t *testing.T) {
	f := func(groupId, key []byte, keyId int) bool {
		// GroupId must not contain any :
		bytes.Replace(groupId, ":", "", -1)

		keyIdBytes := []byte(strconv.Itoa(keyId))
		data := bytes.Join([]byte{
			groupId,
			key,
			keyIdBytes,
		}, []byte(":"))

		parsedAndSerialized := newEventStoreKey(data).bytes()
		return bytes.Compare(parsedAndSerialized, bytes) == 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error("QuickTest failed:", err)
	}

	tests := []evStoreKeySerTest{
		evStoreKeySerTest{
			"g:a",
			"g",
			"a",
			nil,
		},
		evStoreKeySerTest{
			"g:a:1"
			"g",
			"a",
			big.NewInt(1),
		},
		// Test cases where there are multiple colon keys in the
		// middle.
		evStoreKeySerTest{
			"g:a:b:1"
			"g",
			"a:b",
			big.NewInt(1),
		},
		evStoreKeySerTest{
			"g:a:b"
			"g",
			"a:b",
			nil
		},
		evStoreKeySerTest{
			"h:a:b:2"
			"h",
			"a:b",
			big.NewInt(2),
		},
		evStoreKeySerTest{
			"h:a:b:c1"
			"h",
			"a:b:c1",
			nil
		},
		evStoreKeySerTest{
			"h:a:b:11"
			"h",
			"a:b",
			big.NewInt(11),
		},
	}
	for _, test := range tests {
		key := newEventStoreKey([]byte(test.origin))
		if bytes.Compare(key.groupKey, []byte(test.group) != 0 {
			t.Error("Wrong groupkey for:", test.origin)
			t.Error("Expected:", test.group)
			t.Error("Got:     ", test.groupKey)
		}
		if bytes.Compare(key.key, []byte(test.key) != 0 {
			t.Error("Wrong key for:", test.origin)
			t.Error("Expected:", test.group)
			t.Error("Got:     ", test.groupKey)
		}
		switch {
		case test.keyId != nil && key.keyId == nil:
			t.Error("KeyId was nil, not expected.")
		case test.keyId == nil && key.keyId != nil:
			t.Error("KeyId was not nil, expected it to be.")
		case test.keyId != nil && key.keyId != nil:
			if test.keyId.Cmp(key.keyId) != 0 {
				t.Error("Wrong keyId.")
				t.Error("Expected:", test.keyId.String())
				t.Error("Was:     ", key.keyId.String())
			}
	}

}
