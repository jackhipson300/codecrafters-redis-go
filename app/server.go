package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		message := make([]byte, 128)
		_, err := conn.Read(message)
		fmt.Println(message)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}

		conn.Write([]byte("+PONG\r\n"))
	}
}
