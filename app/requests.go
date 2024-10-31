package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type RequestHeader struct {
	size              int
	requestApiKey     int16
	requestApiVersion int16
	correlationID     int32
	clientID          string
}

type RequestMessage struct {
	header RequestHeader
}

func (h RequestHeader) validate() ErrorCode {
	if !(int(h.requestApiVersion) >= 0 && int(h.requestApiVersion) <= 4) {
		return UNSUPPORTED_VERSION
	}
	return NONE
}

func (h *RequestHeader) deserialize(header []byte) {
	h.requestApiKey = int16(binary.BigEndian.Uint16(header[:2]))
	h.requestApiVersion = int16(binary.BigEndian.Uint16(header[2:4]))
	h.correlationID = int32(binary.BigEndian.Uint32(header[4:8]))
	h.clientID = string(header[8:])
}

func getRequestMessage(conn net.Conn) RequestMessage {
	sizeBytes := make([]byte, 4)
	_, err := io.ReadFull(conn, sizeBytes)
	checkError(err)

	size := int(binary.BigEndian.Uint32(sizeBytes))
	data := make([]byte, size)
	_, err = io.ReadFull(conn, data)
	checkError(err)

	header := RequestHeader{size: size}
	header.deserialize(data)

	return RequestMessage{
		header: header,
	}
}

func (r RequestMessage) printHeader() {
	fmt.Println("Received request. Request header:")
	fmt.Println("size:", r.header.size)
	fmt.Println("requestApiKey:", r.header.requestApiKey)
	fmt.Println("requestApiVersion:", r.header.requestApiVersion)
	fmt.Println("correlationID:", r.header.correlationID)
	fmt.Println("clientID:", r.header.clientID)
}