// Tests for rewindd/eventstore
package eventstore


import (
	"testing"
)


type compareTest struct {
	a string
	b string
	expectedResult int
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

