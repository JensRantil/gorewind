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


type evStoreKeySerTest struct {
	origin string
	group string
	key string
	keyId byteCounter
}

func TestEventStoreKeySerialization(t *testing.T) {
	t.Parallel()

	f := func(groupId, key, bKeyId []byte) bool {
		// GroupId must not contain any :
		groupId = bytes.Replace(groupId, groupSep, []byte(""), -1)
		if bytes.Contains(groupId, groupSep) {
			t.Log("It still contained separator:", groupSep)
			t.Log("Bytes:", groupId)
			return false
		}

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
