package main

import (
	"bytes"
	"fmt"
	"net"
)

func waitForResponse(conn net.Conn, expectedRes string) error {
	buffer := make([]byte, 64)
	if _, err := conn.Read(buffer); err != nil {
		return fmt.Errorf("error reading from master: %w", err)
	}

	endIndex := bytes.IndexByte(buffer, '\r')
	if endIndex == -1 {
		return fmt.Errorf("error reading from master: buffer exceeded")
	}

	response := string(buffer[:endIndex])
	if response != expectedRes {
		return fmt.Errorf("unexpected response: expected %s, got %s", expectedRes, response)
	}

	return nil
}

func performHandshake(conn net.Conn) error {
	if _, err := conn.Write([]byte("*1\r\n" + toRespStr("PING"))); err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}
	if err := waitForResponse(conn, "+PONG"); err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}

	replconfPort := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("listening-port") + toRespStr(configParams["port"])
	replconfCapa := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("capa") + toRespStr("psync2")
	if _, err := conn.Write([]byte(replconfPort)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if _, err := conn.Write([]byte(replconfCapa)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if err := waitForResponse(conn, "+OK"); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
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
