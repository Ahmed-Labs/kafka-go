package main

import (
	"bytes"
	"encoding/binary"
)

func encodeCompactString(s string) []byte {
	encoded := []byte{}
	strLen := len(s) + 1
	encoded = append(encoded, byte(strLen))
	encoded = append(encoded, []byte(s)...)

	return encoded
}

func decodeSignedVarint(n int) int {
	return (n >> 1) ^ -(n & 0x1)
}

func readSignedVarint(buf *bytes.Buffer) int {
	var res int
	const (
		SEGMENT_BITS = 0x7F
		CONTINUE_BIT = 0x80
	)

	position := 0
	for {
		seg, err := buf.ReadByte()
		checkError(err)

		res |= int(seg&SEGMENT_BITS) << position

		if seg&CONTINUE_BIT == 0 {
			break
		}

		position += 7
	}

	return decodeSignedVarint(res)
}

// Read encoded compact array
//
// elementSize: size of element in bytes
func readCompactArray[T any](buf *bytes.Buffer) []T {
	var lengthByte byte
	err := binary.Read(buf, binary.BigEndian, &lengthByte)
	checkError(err)

	length := int(lengthByte) - 1
	if length < 0 {
		return nil
	} else if length == 0 {
		return []T{}
	}

	out := []T{}

	for range length {
		var ele T
		err = binary.Read(buf, binary.BigEndian, &ele)
		checkError(err)
		out = append(out, ele)
	}

	return out
}

func encodeCompactArray[T uint16 | uint32, E ~int32 | int](arr []E, appendBits func(b []byte, v T) []byte) []byte {
	if arr == nil {
		return []byte{0}
	}
	arrLen := len(arr)
	if arrLen == 0 {
		return []byte{1}
	}

	res := []byte{byte(arrLen + 1)}

	for _, ele := range arr {
		res = appendBits(res, T(ele))
	}

	return res
}
