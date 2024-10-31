package main

import (
	"fmt"
	"net"
	"os"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
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

	responseMessage := ResponseMessage{}
	responseMessage.header = ResponseHeader{
		correlationID: requestMessage.header.correlationID,
	}

	if err := requestMessage.header.validate(); err != NONE {
		responseMessage.errorCode = UNSUPPORTED_VERSION
	}

	responseMessage.body = NewResponseBody(requestMessage.header.requestApiKey)
	serializedResponseMessage := responseMessage.serialize()
	sendResponse(conn, serializedResponseMessage)
}
