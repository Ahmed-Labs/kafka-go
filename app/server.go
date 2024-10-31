package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type ResponseHeader struct {
	correlationID int32
}

type ResponseBody struct {
	errorCode ErrorCode
}

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

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func (h RequestHeader) validate() ErrorCode {
	if !(int(h.requestApiVersion) >= 0 && int(h.requestApiVersion) <= 2) {
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

func sendResponse(conn net.Conn, responseMessage []byte) {
	fmt.Println("Sending:", responseMessage)
	_, err := conn.Write(responseMessage)
	checkError(err)
}

func (rb ResponseBody) serialize() []byte {
	body := []byte{}

	if rb.errorCode != NONE {
		errCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errCodeBytes, uint16(rb.errorCode))
		return errCodeBytes
	}

	return body
}

func (rs ResponseHeader) serialize() []byte {
	correlationID := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationID, uint32(rs.correlationID))
	return correlationID
}

func buildResponseMessage(header ResponseHeader, body ResponseBody) []byte {
	serializedHeader := header.serialize()
	serializedBody := body.serialize()
	messageSize := len(serializedHeader) + len(serializedBody)

	message := make([]byte, 4+messageSize) // messageSize itself is 4 bytes

	binary.BigEndian.PutUint32(message, uint32(messageSize))
	copy(message[4:], serializedHeader)
	copy(message[4+len(serializedHeader):], serializedBody)

	return message
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	requestMessage := getRequestMessage(conn)

	responseBody := ResponseBody{}
	if err := requestMessage.header.validate(); err != NONE {
		responseBody.errorCode = UNSUPPORTED_VERSION
	}

	responseHeader := ResponseHeader{
		correlationID: requestMessage.header.correlationID,
	}

	responseMessage := buildResponseMessage(responseHeader, responseBody)
	sendResponse(conn, responseMessage)
}
