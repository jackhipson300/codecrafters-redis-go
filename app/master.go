package main

import (
	"fmt"
	"net"
)

func performHandshake(conn net.Conn) error {
	if _, err := conn.Write([]byte("*1\r\n" + toRespStr("PING"))); err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}

	return nil
}

func handleMaster(conn net.Conn) {
	if err := performHandshake(conn); err != nil {
		fmt.Println("Error performing handshake (will close)", err.Error())
		if err := conn.Close(); err != nil {
			fmt.Println("Error closing connection to master", err.Error())
		}
		return
	}
}
