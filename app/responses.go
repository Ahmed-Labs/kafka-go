package main

import (
	"encoding/binary"
	"fmt"
	"net"
)


type ResponseHeader struct {
	correlationID int32
}

type ResponseMessage struct {
	header    ResponseHeader
	body      SerializableResponse
	errorCode ErrorCode
}

type SerializableResponse interface {
	serialize() []byte
}

func NewResponseBody(apiKey int16) SerializableResponse {
	switch apiKey {
	case 18: // ApiVersionsRequest
		apiVersions := make([]ApiVersion, len(supportedApiKeys))
		for i, key := range supportedApiKeys {
			apiVersions[i] = ApiVersion{
				ApiKey:     key,
				MinVersion: minVersion,
				MaxVersion: maxVersion,
			}
		}
		return ApiVersionsResponse{apiVersions: apiVersions}
	default:
		return nil
	}
}

func (rs ResponseHeader) serialize() []byte {
	correlationID := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationID, uint32(rs.correlationID))
	return correlationID
}

func (r ResponseMessage) serialize() []byte {
	serializedHeader := r.header.serialize()
	serializedBody := make([]byte, 2)

	binary.BigEndian.PutUint16(serializedBody, uint16(r.errorCode))

	if r.body != nil {
		body := r.body.serialize()
		serializedBody = append(serializedBody, body...)
	}

	serializedBody = append(serializedBody, []byte{0, 0, 0, 0}...) // Throttle time
	serializedBody = append(serializedBody, 0) // Tag buffer

	messageSize := len(serializedHeader) + len(serializedBody)
	message := make([]byte, 4+messageSize) // messageSize itself is 4 bytes

	binary.BigEndian.PutUint32(message, uint32(messageSize))
	copy(message[4:], serializedHeader)
	copy(message[4+len(serializedHeader):], serializedBody)

	return message
}

func sendResponse(conn net.Conn, responseMessage []byte) {
	fmt.Println("Sending:", responseMessage)
	_, err := conn.Write(responseMessage)
	checkError(err)
}
