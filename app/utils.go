package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
)

const nullRespStr = "$-1\r\n"

func toRespStr(raw string) string {
	length := len(raw)
	return fmt.Sprintf("$%d\r\n%s\r\n", length, raw)
}

func generateReplId() string {
	bytes := make([]byte, 40)
	rand.Read(bytes)

	return hex.EncodeToString(bytes)
}

func addToInfoResponse(key string, value string, response *string) {
	*response += "\r\n" + key + ":" + value
}

func write(conn net.Conn, data []byte) (int, error) {
	if configParams["role"] == "master" {
		return conn.Write(data)
	}

	return 0, nil
}
