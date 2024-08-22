package main

import (
	"bytes"
	"fmt"
	"net"
)

func getResponse(conn net.Conn, bufferSize int) (string, error) {
	buffer := make([]byte, bufferSize)
	if _, err := conn.Read(buffer); err != nil {
		return "", fmt.Errorf("error reading from master: %w", err)
	}

	endIndex := bytes.IndexByte(buffer, '\r')
	if endIndex == -1 {
		return "", fmt.Errorf("error reading from master: buffer exceeded")
	}

	return string(buffer[:endIndex]), nil
}

func performHandshake(conn net.Conn) error {
	if _, err := conn.Write([]byte("*1\r\n" + toRespStr("PING"))); err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}
	if response, err := getResponse(conn, 64); response != "+PONG" || err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}

	replconfPort := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("listening-port") + toRespStr(configParams["port"])
	replconfCapa := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("capa") + toRespStr("psync2")
	if _, err := conn.Write([]byte(replconfPort)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if response, err := getResponse(conn, 64); response != "+OK" || err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if _, err := conn.Write([]byte(replconfCapa)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if response, err := getResponse(conn, 64); response != "+OK" || err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}

	psync := "*3\r\n" + toRespStr("PSYNC") + toRespStr("?") + toRespStr("-1")
	if _, err := conn.Write([]byte(psync)); err != nil {
		return fmt.Errorf("error making psync: %w", err)
	}

	response, err := getResponse(conn, 128)
	if response[:11] != "+FULLRESYNC" || err != nil {
		return fmt.Errorf("error making psync: %w", err)
	}

	if _, err := getResponse(conn, 1024); err != nil {
		return fmt.Errorf("error receiving rdb file: %w", err)
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
