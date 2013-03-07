// Tests for rewindd/eventstore
package server


import (
	"testing"
	"bytes"
)


type compareTest struct {
	a string
	b string
	expectedResult int
}

func testComparater(t *testing.T, test compareTest, c EventStreamComparer) {
	testComparator(t, test, c)
	testSeparator(t, test, c)
	testSuccessor(t, test.a, c)
	testSuccessor(t, test.b, c)
}

func testSeparator(t *testing.T, test compareTest, c EventStreamComparer) {
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

func testComparator(t *testing.T, test compareTest, comparer EventStreamComparer) {
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
		t.Error("Successor was longer: %s", s)
	}
	if comparer.Compare(shorter, bs) < 0 {
		t.Error("Successor was greater the its origin: %s", s)
	}
}

func TestComparator(t *testing.T) {
	comparer := EventStreamComparer{}
	tests := []compareTest{
		compareTest{"g:a", "g:a", 0},
		compareTest{"g:a", "g:b", -1},
		compareTest{"g:a", "h:a", -1},
		compareTest{"g:a:1", "g:a:1", 0},
		compareTest{"g:a:1", "h:a:1", -1},
		compareTest{"g:a:1", "h:a:2", -1},
		compareTest{"g:a:1", "h:a:11", -1},
		compareTest{"g:a:b:1", "g:a:b:11", -1},

		// Test cases where there are multiple comma keys in the
		// middle.
		compareTest{"g:a:b", "g:a:b", 0},
		compareTest{"g:a:b", "g:b:c", -1},
		compareTest{"g:a:b", "h:a:b", -1},
		compareTest{"g:a:b:1", "g:a:b:1", 0},
		compareTest{"g:a:q:1", "h:a:q:1", -1},
		compareTest{"g:a:b:1", "h:a:b:2", -1},
		compareTest{"g:a:b:1", "h:a:b:11", -1},
	}
	for _, test := range(tests) {
		testComparator(t, test, comparer)
		if test.expectedResult != 0 {
			// Testing the inverted case
			invertedTest := compareTest{
				test.b,
				test.a,
				-test.expectedResult,
			}
			testComparator(t, invertedTest, comparer)
		}
	}
}

