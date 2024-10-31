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

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func getRequestBody(conn net.Conn) []byte {
	sizeBytes := make([]byte, 4)
	_, err := io.ReadFull(conn, sizeBytes)
	checkError(err)

 	size := int(binary.BigEndian.Uint32(sizeBytes))

	data := make([]byte, size)
	_, err = io.ReadFull(conn, data)
	checkError(err)

	return data
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

	message := make([]byte, 4 + messageSize) // messageSize itself is 4 bytes
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
	
	responseHeader := ResponseHeader{ correlationID: 7 }
	responseMessage := buildResponseMessage(responseHeader)
	sendResponse(conn, responseMessage)
}
