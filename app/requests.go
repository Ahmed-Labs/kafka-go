package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type RequestHeader struct {
	size              int
	requestApiKey     ApiKey
	requestApiVersion int16
	correlationID     int32
	clientID          string
}

type RequestBody interface {
	deserialize([]byte)
}

type RequestMessage struct {
	header RequestHeader
	body   RequestBody
}

func getRequestBody(apiKey ApiKey) RequestBody {
	switch apiKey {
	case DESCRIBE_TOPIC_PARTITIONS:
		return &DescribeTopicPartitionsRequest{}
	case FETCH:
		return &FetchRequest{}
	default:
		return nil
	}
}
func (h *RequestHeader) deserialize(header []byte) int {
	h.requestApiKey = ApiKey(binary.BigEndian.Uint16(header[:2]))
	h.requestApiVersion = int16(binary.BigEndian.Uint16(header[2:4]))
	h.correlationID = int32(binary.BigEndian.Uint32(header[4:8]))

	clientIDLength := int(binary.BigEndian.Uint16(header[8:10]))
	h.clientID = string(header[10 : 10+clientIDLength])

	tagBufferLength := header[10+clientIDLength]
	requestBodyIdx := 10 + clientIDLength + 1

	if tagBufferLength != 0 {
		requestBodyIdx += int(tagBufferLength)
	}
	// Returns index to the start of request body
	return requestBodyIdx
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
	bodyIdx := header.deserialize(data)

	body := getRequestBody(header.requestApiKey)
	if body != nil {
		body.deserialize(data[bodyIdx:])
	}
	fmt.Printf("Request Body: %+v\n", body)

	return RequestMessage{
		header: header,
		body:   body,
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
