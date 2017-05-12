// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"

	"github.com/pingcap/tidb/util/hack"
)

func parseFromString(s string) (JSON, error) {
	if len(s) == 0 {
		return nil, ErrInvalidJSONText.GenByArgs("The document is empty")
	}
	var in interface{}
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil, ErrInvalidJSONText.GenByArgs(err)
	}
	if j, ok := in.(JSON); ok {
		return j, nil
	} else {
		return nil, ErrInvalidJSONText.GenByArgs(s)
	}
}

func dumpToString(j JSON) string {
	bytes, _ := json.Marshal(j)
	return strings.Trim(string(bytes), "\n")
}

type jsonBool bool
type jsonString string
type jsonObject map[string]JSON
type jsonArray []JSON
type jsonDouble float64

func serialize(j JSON) []byte {
	if j == nil {
		return []byte{0x04, 0x00}
	}
	var buffer = new(bytes.Buffer)
	buffer.WriteByte(j.getTypeCode())
	j.writeBinaryRepresentation(buffer)
	return buffer.Bytes()
}

func deserialize(data []byte) JSON {
	if data[0] == 0x04 && data[1] == 0x00 {
		return nil
	}
	return nil
	// return pop(data[0], data[1:])
}

func (b *jsonBool) getTypeCode() byte {
	return 0x04
}

func (b *jsonBool) writeBinaryRepresentation(buffer *bytes.Buffer) {
	if *b {
		buffer.WriteByte(0x01)
	} else {
		buffer.WriteByte(0x02)
	}
}

func (b *jsonBool) readBinaryRepresentation(data []byte) {
	*b = data[0] == 0x01
}

func (f *jsonDouble) getTypeCode() byte {
	return 0x0b
}

func (f *jsonDouble) writeBinaryRepresentation(buffer *bytes.Buffer) {
	binary.Write(buffer, binary.LittleEndian, *f)
}

func (f *jsonDouble) readBinaryRepresentation(data []byte) {
	var reader = bytes.NewReader(data)
	binary.Read(reader, binary.LittleEndian, f)
}

func (s *jsonString) getTypeCode() byte {
	return 0x0c
}

func (s *jsonString) writeBinaryRepresentation(buffer *bytes.Buffer) {
	var ss = string(*s)
	var varIntBuf = make([]byte, 9)
	var varIntLen = binary.PutUvarint(varIntBuf, uint64(len(hack.Slice(ss))))
	buffer.Write(varIntBuf[0:varIntLen])
	buffer.Write(hack.Slice(ss))
}

func (s *jsonString) readBinaryRepresentation(data []byte) {
	var reader = bytes.NewReader(data)
	length, _ := binary.ReadUvarint(reader)
	var buf = make([]byte, length)
	reader.Read(buf)
	*s = jsonString(string(buf))
}

// func (m *map[string]JSON) getTypeCode() {
// 	return 0x01
// }
//
// func (m *map[string]JSON) writeBinaryRepresentation(buffer *bytes.Buffer) {
// 	pushObject(buffer, m)
// }
//
// func (a *[]JSON) getTypeCode() {
// 	return 0x03
// }
//
// func (a *[]JSON) writeBinaryRepresentation(buffer *bytes.Buffer) {
// 	pushArray(buffer, a)
// }
