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
	"log"
	"math"
	"encoding/base64"
)

// The separator used for separating into the different eventStoreKey
// fields.
var groupSep []byte = []byte(":")

// Represents a leveldb key.
type eventStoreKey struct {
	groupKey []byte
	key []byte
	keyId byteCounter
}

// Convert a eventStoreKey to bytes. The returned byte slice is either
// "groupKey:key:keyId" if keyId is non-nil, or "groupKey:key"
// otherwise.
func (v *eventStoreKey) toBytes() []byte {
	var pieces [][]byte
	if bytes.Contains(v.groupKey, groupSep) {
		// Generally, this should not happen since groupSep is
		// purely a constant set by a developer. That said, you
		// could see this check as a runtime assertion.
		log.Panicln("groupKey must not contain groupSep:", v.groupKey)
	}
	if v.keyId != nil {
		pieces = make([][]byte, 3)
		pieces[0] = v.groupKey
		pieces[1] = v.key
		sKeyId := base64.StdEncoding.EncodeToString(v.keyId)
		pieces[2] = []byte(sKeyId)
	} else {
		pieces = make([][]byte, 2)
		pieces[0] = v.groupKey
		pieces[1] = v.key
	}
	return bytes.Join(pieces, groupSep)

}

// Convert a byte slice to a parsed eventStoreKey.
func newEventStoreKey(data []byte) (*eventStoreKey, error) {
	res := new(eventStoreKey)
	pieces := bytes.Split(data, groupSep)
	if len(pieces) > 2 {
		codedKeyId := pieces[len(pieces)-1]
		enc := base64.StdEncoding
		bDecodedKeyId, err := enc.DecodeString(string(codedKeyId))
		if err != nil {
			return nil, err
		}
		res.keyId = bDecodedKeyId
	}
	if len(pieces) > 0 {
		res.groupKey = pieces[0]
	}
	if len(pieces) > 1 {
		var upperIndex int
		if len(pieces) > math.MaxInt32 {
			// Handle the case when len(pieces)>=max(int).
			// Note that `int` can be 64 bit on Go 1.1. Keys
			// are supposed to be small in size, so I don't
			// expect this to be an issue.
			msg := "too many pieces for deserialization"
			return nil, errors.New(msg)
		}
		if res.keyId != nil {
			upperIndex = len(pieces) - 1
		} else {
			upperIndex = len(pieces)
		}
		keyPieces := pieces[1:upperIndex]
		res.key = bytes.Join(keyPieces, groupSep)
	}
	return res, nil
}

// Compare to another eventStoreKey. Returns -1 if this one is smaller
// than o2, 0 same, or 1 is this one is bigger than the previous one.
func (o1 *eventStoreKey) Compare(o2 *eventStoreKey) int {
	if diff := bytes.Compare(o1.groupKey, o2.groupKey); diff != 0 {
		return diff
	}
	if diff := bytes.Compare(o1.key, o2.key); diff != 0 {
		return diff
	}
	switch {
	case o1.keyId != nil && o2.keyId != nil:
		return o1.keyId.Compare(o2.keyId)
	case o1.keyId != nil:
		return -1
	case o2.keyId != nil:
		return 1
	}
	return 0
}
