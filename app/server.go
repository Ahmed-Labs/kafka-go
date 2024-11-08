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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		requestMessage := getRequestMessage(conn)
		requestMessage.printHeader()

		responseMessage := NewResponse(requestMessage)
		sendResponse(conn, *responseMessage)
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}
