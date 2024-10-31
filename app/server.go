package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type ResponseHeader struct {
	correlationID uint32
}

type RequestHeader struct {
	size int
	requestApiKey     uint16
	requestApiVersion uint16
	correlationID     uint32
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

func (h *RequestHeader) deserialize(header []byte) {
	h.requestApiKey = binary.BigEndian.Uint16(header[:2])
	h.requestApiVersion = binary.BigEndian.Uint16(header[2:4])
	h.correlationID = binary.BigEndian.Uint32(header[4:8])
	h.clientID = string(header[8:])
}

func getRequestMessage(conn net.Conn) RequestMessage{
	sizeBytes := make([]byte, 4)
	_, err := io.ReadFull(conn, sizeBytes)
	checkError(err)

	size := int(binary.BigEndian.Uint32(sizeBytes))

	data := make([]byte, size)
	_, err = io.ReadFull(conn, data)
	checkError(err)

	header := RequestHeader{ size: size }
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

func (rs ResponseHeader) serialize() []byte {
	correlationID := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationID, rs.correlationID)
	return correlationID
}

func buildResponseMessage(header ResponseHeader) []byte {
	serializedHeader := header.serialize()
	messageSize := len(serializedHeader)

	message := make([]byte, 4+messageSize) // messageSize itself is 4 bytes
	binary.BigEndian.PutUint32(message, uint32(messageSize))
	copy(message[4:], serializedHeader)

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
	
	responseHeader := ResponseHeader{
		correlationID: requestMessage.header.correlationID,
	}
	responseMessage := buildResponseMessage(responseHeader)
	sendResponse(conn, responseMessage)
}
