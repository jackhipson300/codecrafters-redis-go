package main

import (
	"bufio"
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

func toRespArr(strs ...string) string {
	respArr := fmt.Sprintf("*%d\r\n", len(strs))
	for _, str := range strs {
		respArr += toRespStr(str)
	}

	return respArr
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

func readResp(reader *bufio.Reader) (string, error) {
	message, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return message[:len(message)-2], nil
}

func findMostRecentEntryByTimestamp(stream Stream, search int) (StreamEntry, bool) {
	found := false
	entry := StreamEntry{}
	minSeqNumber := -1

	for _, curr := range stream.entries {
		if curr.timestamp == search && curr.sequenceNumber > minSeqNumber {
			entry = curr
			minSeqNumber = curr.sequenceNumber
			found = true
		}
	}

	return entry, found
}
