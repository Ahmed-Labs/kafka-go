package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

type ResponseHeader interface {
	serialize() []byte
}

type ResponseHeaderV0 struct {
	correlationID int32
}

type ResponseHeaderV1 struct {
	correlationID int32
	tagBuffer     byte
}

type SerializableResponse interface {
	serialize() []byte
}

type ResponseMessage struct {
	header ResponseHeader
	body   SerializableResponse
}

func NewResponse(req RequestMessage) *ResponseMessage {
	response := ResponseMessage{}
	apiKey := req.header.requestApiKey

	switch apiKey {
	case API_VERSIONS:
		response.body = buildApiVersionsResponse(req)
		response.header = ResponseHeaderV0{correlationID: req.header.correlationID}
	case DESCRIBE_TOPIC_PARTITIONS:
		response.body = buildDescribeTopicPartitionsResponse(req)
		response.header = ResponseHeaderV1{correlationID: req.header.correlationID}
	}

	return &response
}

func (rs ResponseHeaderV0) serialize() []byte {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(rs.correlationID))
	return header
}

func (rs ResponseHeaderV1) serialize() []byte {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(rs.correlationID))
	header = append(header, rs.tagBuffer)
	return header
}

func (r ResponseMessage) serialize() []byte {
	serializedHeader := r.header.serialize()
	serializedBody := []byte{}

	if r.body != nil {
		body := r.body.serialize()
		serializedBody = append(serializedBody, body...)
	}

	messageSize := len(serializedHeader) + len(serializedBody)
	message := make([]byte, 4+messageSize) // messageSize itself is 4 bytes

	fmt.Println("Header", serializedHeader)
	fmt.Println("Body:", serializedBody)

	binary.BigEndian.PutUint32(message, uint32(messageSize))
	copy(message[4:], serializedHeader)
	copy(message[4+len(serializedHeader):], serializedBody)

	return message
}

func sendResponse(conn net.Conn, responseMessage ResponseMessage) {
	serializedMsg := responseMessage.serialize()
	_, err := conn.Write(serializedMsg)
	checkError(err)
	fmt.Println("Sent:", serializedMsg)
}
