package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

func performHandshake(conn net.Conn, reader *bufio.Reader) error {

	rdbMutex.Lock()
	defer rdbMutex.Unlock()

	if _, err := conn.Write([]byte("*1\r\n" + toRespStr("PING"))); err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}
	if response, err := readResp(reader); response != "+PONG" || err != nil {
		return fmt.Errorf("error making ping: %w", err)
	}

	replconfPort := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("listening-port") + toRespStr(configParams["port"])
	replconfCapa := "*3\r\n" + toRespStr("REPLCONF") + toRespStr("capa") + toRespStr("psync2")
	if _, err := conn.Write([]byte(replconfPort)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if response, err := readResp(reader); response != "+OK" || err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if _, err := conn.Write([]byte(replconfCapa)); err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}
	if response, err := readResp(reader); response != "+OK" || err != nil {
		return fmt.Errorf("error making replconf: %w", err)
	}

	psync := "*3\r\n" + toRespStr("PSYNC") + toRespStr("?") + toRespStr("-1")
	if _, err := conn.Write([]byte(psync)); err != nil {
		return fmt.Errorf("error making psync: %w", err)
	}

	if response, err := readResp(reader); response[:11] != "+FULLRESYNC" || err != nil {
		return fmt.Errorf("error making psync: %w", err)
	}

	lengthStr, err := readResp(reader)
	if err != nil {
		return fmt.Errorf("error receiving rdb file length: %w", err)
	}

	length, err := strconv.Atoi(lengthStr[1:])
	if err != nil {
		return fmt.Errorf("error receiving rdb file length: %w", err)
	}

	buffer := make([]byte, length)
	if _, err := reader.Read(buffer); err != nil {
		return fmt.Errorf("error receiving rdb file: %w", err)
	}

	rdbFile = buffer

	return nil
}

func handleMaster(conn net.Conn, reader *bufio.Reader) {
	if err := performHandshake(conn, reader); err != nil {
		fmt.Println("Error performing handshake (will close)", err.Error())
		if err := conn.Close(); err != nil {
			fmt.Println("Error closing connection to master", err.Error())
		}
		return
	}

	handleClient(conn, reader)
}
