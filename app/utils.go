package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
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

func findMostRecentEntryByTimestamp(stream Stream, search int64) (StreamEntry, bool) {
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

func getEntriesInRange(stream Stream, startId string, endId string) ([]StreamEntry, error) {
	var err error
	entries := []StreamEntry{}

	startIdParts := strings.Split(startId, "-")
	endIdParts := strings.Split(endId, "-")

	startSeqNum := 0
	endSeqNum := math.MaxInt
	if len(startIdParts) == 2 && startId != "-" {
		startSeqNum, err = strconv.Atoi(startIdParts[1])
		if err != nil {
			return entries, err
		}
	}
	if len(endIdParts) == 2 && endId != "+" {
		endSeqNum, err = strconv.Atoi(endIdParts[1])
		if err != nil {
			return entries, err
		}
	}

	startTimestamp := int64(0)
	if startId != "-" {
		startTimestamp, err = strconv.ParseInt(startIdParts[0], 10, 64)
		if err != nil {
			return entries, err
		}
	}
	endTimestamp := int64(math.MaxInt64)
	if endId != "+" {
		endTimestamp, err = strconv.ParseInt(endIdParts[0], 10, 64)
		if err != nil {
			return entries, err
		}
	}

	for _, entry := range stream.entries {
		validTimestamp := entry.timestamp >= startTimestamp && entry.timestamp <= endTimestamp
		validSeqNum := entry.sequenceNumber >= startSeqNum && entry.sequenceNumber <= endSeqNum
		if validTimestamp && validSeqNum {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func findMostRecentEntryId(stream *Stream) (int64, int, bool) {
	mostRecentTimestamp := int64(-1)
	mostRecentSeqNum := -1

	if len(stream.entries) == 0 {
		return int64(0), 0, false
	}

	for _, entry := range stream.entries {
		if entry.timestamp == mostRecentTimestamp {
			if entry.sequenceNumber > mostRecentSeqNum {
				mostRecentSeqNum = entry.sequenceNumber
			}
		}
		if entry.timestamp > mostRecentTimestamp {
			mostRecentTimestamp = entry.timestamp
			mostRecentSeqNum = entry.sequenceNumber
		}
	}

	return mostRecentTimestamp, mostRecentSeqNum, true
}
