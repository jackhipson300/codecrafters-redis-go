package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const emptyRdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

const xaddEntryIdOlderThanLastErr = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
const xaddEntryIdZeroErr = "-ERR The ID specified in XADD must be greater than 0-0\r\n"
const incrNotNumErr = "-ERR value is not an integer or out of range\r\n"
const execNotInQueueModeErr = "-ERR EXEC without MULTI\r\n"
const discardNotInQueueModeErr = "-ERR DISCARD without MULTI\r\n"

var xreadBlockMutex = sync.Mutex{}
var xreadBlockSignal = sync.NewCond(&xreadBlockMutex)

var commands map[string]func([]string, *Client) (string, error)

func echoCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing echo: no args")
	}

	return toRespStr(args[0]), nil
}

func pingCommand(args []string, client *Client) (string, error) {
	return "+PONG\r\n", nil
}

func setCommand(args []string, client *Client) (string, error) {
	now := time.Now()

	setHasOccurred = true

	ackLock.Lock()
	numAcksSinceLasSet = 0
	ackLock.Unlock()

	if len(args) < 2 {
		return "", fmt.Errorf("error performing set: not enough args")
	}

	expiresAt := int64(-1)
	for i, arg := range args {
		switch strings.ToLower(arg) {
		case "px":
			if i+1 < len(args) {
				ttl, err := strconv.Atoi(args[i+1])
				if err != nil {
					ttl = 0
				}
				expiresAt = now.UnixMilli() + int64(ttl)
			}
		}
	}

	keyValueCache[args[0]] = CacheItem{
		value:     args[1],
		expiresAt: expiresAt,
		itemType:  "string",
	}
	return "+OK\r\n", nil
}

func getCommand(args []string, client *Client) (string, error) {
	now := time.Now().UnixMilli()

	if len(args) == 0 {
		return "", fmt.Errorf("error performing get: no args")
	}

	var value string
	var expiresAt int64
	entry, exists := keyValueCache[args[0]]
	if exists {
		value = entry.value
		expiresAt = entry.expiresAt
	} else {
		keysMap, err := getKeys()
		if err != nil {
			fmt.Println(err.Error())
		}

		valueInfo, exists := keysMap[args[0]]
		if exists {
			expiresAt = valueInfo.expiresAt
			value, err = getValue(valueInfo)
			if err != nil {
				return "", fmt.Errorf("error performing get: %w", err)
			}
		}
	}

	if value == "" {
		return nullRespStr, nil
	}

	writeVal := toRespStr(value)
	if expiresAt != -1 && now >= expiresAt {
		fmt.Println("Entry expired: ", now, entry.expiresAt)
		writeVal = nullRespStr
	}

	return writeVal, nil
}

func configCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing config: no args")
	}

	if len(args) < 2 {
		return "", fmt.Errorf("error performing config get: not enough args")
	}

	response := nullRespStr
	value, exists := configParams[args[1]]
	if exists && value != "" {
		response = fmt.Sprintf("*2\r\n%s%s", toRespStr(args[1]), toRespStr(value))
	}

	return response, nil
}

func keysCommand(args []string, client *Client) (string, error) {
	keysMap, err := getKeys()
	if err != nil {
		return "", err
	}

	response := fmt.Sprintf("*%d\r\n", len(keysMap))
	for key := range keysMap {
		response += toRespStr(key)
	}

	return response, nil
}

func infoCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing info: no args")
	}

	response := "role:" + configParams["role"]
	if configParams["role"] == "master" {
		addToInfoResponse("master_replid", configParams["replId"], &response)
		addToInfoResponse("master_repl_offset", configParams["replOffset"], &response)
	}

	return toRespStr(response), nil
}

func replconfCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing replconf: no args")
	}

	switch strings.ToLower(args[0]) {
	case "listening-port":
		return "+OK\r\n", nil
	case "capa":
		return "+OK\r\n", nil
	case "getack":
		return toRespArr("REPLCONF", "ACK", strconv.Itoa(bytesProcessed)), nil
	case "ack":
		if len(args) < 2 {
			return "", fmt.Errorf("error handling replconf ack: not enough args")
		}

		ackLock.Lock()
		numAcksSinceLasSet++
		ackLock.Unlock()
		return "", nil
	}

	return "", fmt.Errorf("unsupported replconf")
}

func psyncCommand(args []string, client *Client) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("error performing psync: not enough args")
	}

	if args[0] == "?" {
		response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", configParams["replId"])
		if _, err := client.conn.Write([]byte(response)); err != nil {
			return "", fmt.Errorf("error performing psync: %w", err)
		}

		emptyRdbAsBytes, _ := hex.DecodeString(emptyRdb)
		fileResponse := append([]byte(fmt.Sprintf("$%d\r\n", len(emptyRdbAsBytes))), emptyRdbAsBytes...)
		if _, err := client.conn.Write(fileResponse); err != nil {
			return "", fmt.Errorf("error performing psync: %w", err)
		}

		replicasLock.Lock()
		defer replicasLock.Unlock()

		replicas = append(replicas, client.conn)
	}

	return "", nil
}

func waitCommand(args []string, client *Client) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("error performing wait: not enough args")
	}

	if !setHasOccurred {
		fmt.Println("Set has not occurred, sending 0")
		replicasLock.Lock()
		defer replicasLock.Unlock()

		numAcks := len(replicas)

		response := fmt.Sprintf(":%d\r\n", numAcks)
		return response, nil
	}

	replicasLock.Lock()
	for _, replica := range replicas {
		if _, err := replica.Write([]byte(toRespArr("REPLCONF", "GETACK", "*"))); err != nil {
			fmt.Println("Failed to getack after relaying command to replica", err.Error())
		}
	}
	replicasLock.Unlock()

	requiredAcks, err := strconv.Atoi(args[0])
	if err != nil {
		return "", fmt.Errorf("error performing wait: %w", err)
	}
	timeoutMS, err := strconv.Atoi(args[1])
	if err != nil {
		return "", fmt.Errorf("error performing wait: %w", err)
	}

	timeout := time.Duration(timeoutMS) * time.Millisecond

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutChannel := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			if numAcksSinceLasSet >= requiredAcks {
				response := fmt.Sprintf(":%d\r\n", numAcksSinceLasSet)
				return response, nil
			}
		case <-timeoutChannel:
			response := fmt.Sprintf(":%d\r\n", numAcksSinceLasSet)
			return response, nil
		}
	}
}

func typeCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing type command: no args")
	}

	entry, exists := keyValueCache[args[0]]
	itemType := entry.itemType
	if !exists {
		_, exists := streamCache[args[0]]
		if exists {
			itemType = "stream"
		} else if len(rdbFile) != 0 {
			keys, err := getKeys()
			if err != nil {
				return "", fmt.Errorf("error performing type command: %w", err)
			}
			_, exists = keys[args[0]]
			if exists {
				itemType = "string"
			}
		}
	}

	if itemType != "" {
		return fmt.Sprintf("+%s\r\n", itemType), nil
	} else {
		return "+none\r\n", nil
	}
}

func xaddCommand(args []string, client *Client) (string, error) {
	xreadBlockMutex.Lock()
	defer xreadBlockMutex.Unlock()
	defer xreadBlockSignal.Signal()

	var err error
	if len(args) < 2 {
		return "", fmt.Errorf("error performing xadd: not enough args")
	}

	streamId := args[0]
	stream, exists := streamCache[streamId]
	if !exists {
		stream = &Stream{}
		streamCache[streamId] = stream
	}

	var millisecondsTime int64
	var sequenceNumber int
	entryId := args[1]

	if entryId == "*" {
		millisecondsTime = time.Now().UnixMilli()
		previousEntry, exists := findMostRecentEntryByTimestamp(*stream, millisecondsTime)
		if exists {
			sequenceNumber = previousEntry.sequenceNumber + 1
		}
	} else {
		entryIdParts := strings.Split(entryId, "-")
		if len(entryIdParts) < 2 {
			return "", fmt.Errorf("error performing xadd: invalid entry id")
		}

		millisecondsTime, err = strconv.ParseInt(entryIdParts[0], 10, 64)
		if err != nil {
			return "", fmt.Errorf("error performing xadd: invalid entry id: %w", err)
		}

		sequenceNumber = 0
		if millisecondsTime == 0 {
			sequenceNumber = 1
		}

		if entryIdParts[1] == "*" {
			previousEntry, exists := findMostRecentEntryByTimestamp(*stream, millisecondsTime)
			if exists {
				sequenceNumber = previousEntry.sequenceNumber + 1
			}
		} else {
			sequenceNumber, err = strconv.Atoi(entryIdParts[1])
			if err != nil {
				return "", fmt.Errorf("error performing xadd: invalid entry id: %w", err)
			}
		}
	}

	if millisecondsTime == 0 && sequenceNumber == 0 && len(stream.entries) > 0 {
		return xaddEntryIdZeroErr, nil
	}

	millisecondsTimeInvalid := stream.lastMillisecondsTime > millisecondsTime
	sequenceNumberInvalid := stream.lastMillisecondsTime == millisecondsTime &&
		stream.lastSequenceNumber >= sequenceNumber
	if millisecondsTimeInvalid || sequenceNumberInvalid {
		return xaddEntryIdOlderThanLastErr, nil
	}

	stream.lastMillisecondsTime = millisecondsTime
	stream.lastSequenceNumber = sequenceNumber

	entry := StreamEntry{
		timestamp:      millisecondsTime,
		sequenceNumber: sequenceNumber,
		values:         map[string]string{},
	}
	stream.entries = append(stream.entries, entry)

	for i := 2; i < len(args); i += 2 {
		key := args[i]
		value := args[i+1]
		entry.values[key] = value
	}

	entryId = fmt.Sprintf("%d-%d", millisecondsTime, sequenceNumber)
	return toRespStr(entryId), nil
}

func xrangeCommand(args []string, client *Client) (string, error) {
	var err error
	if len(args) < 3 {
		return "", fmt.Errorf("error performing xrange: not enough args")
	}

	streamId := args[0]
	stream, exists := streamCache[streamId]
	if !exists {
		return "*0\r\n", nil
	}

	startId := args[1]
	endId := args[2]

	validEntries, err := getEntriesInRange(*stream, startId, endId)
	if err != nil {
		return "", err
	}

	innerRespArrs := []string{}
	for _, entry := range validEntries {
		entryId := fmt.Sprintf("%d-%d", entry.timestamp, entry.sequenceNumber)
		respParts := []string{}
		for key, value := range entry.values {
			respParts = append(respParts, key, value)
		}

		respStr := "*2\r\n" + toRespStr(entryId) + toRespArr(respParts...)
		innerRespArrs = append(innerRespArrs, respStr)
	}

	response := fmt.Sprintf("*%d\r\n", len(innerRespArrs)) + strings.Join(innerRespArrs, "")
	return response, nil
}

func xreadCommand(args []string, client *Client) (string, error) {
	var err error
	if len(args) < 3 {
		return "", fmt.Errorf("error performing xread: not enough args")
	}

	shouldBlock := args[0] == "block"
	blockDelayStr := args[1]
	blockDelayMs := int64(0)
	streamsOffset := 1
	if shouldBlock {
		blockDelayMs, err = strconv.ParseInt(blockDelayStr, 10, 64)
		if err != nil {
			return "", err
		}
		streamsOffset = 3
	}

	streamIds := []string{}
	for i := streamsOffset; i < len(args); i++ {
		if strings.Contains(args[i], "-") || args[i] == "$" {
			break
		}
		streamIds = append(streamIds, args[i])
	}

	streams := []*Stream{}
	for _, streamId := range streamIds {
		stream, exists := streamCache[streamId]
		if !exists {
			return "*0\r\n", nil
		}
		streams = append(streams, stream)
	}

	var timestamp int64
	var seqNum int
	givenEntryId := args[len(streamIds)+streamsOffset]

	if givenEntryId == "$" {
		var exists bool
		timestamp, seqNum, exists = findMostRecentEntryId(streams[0])
		if !exists {
			timestamp = 0
			seqNum = 0
		}
	} else {
		idParts := strings.Split(givenEntryId, "-")
		timestamp, err = strconv.ParseInt(idParts[0], 10, 64)
		if err != nil {
			return "", err
		}
		seqNum, err = strconv.Atoi(idParts[1])
		if err != nil {
			return "", err
		}
	}

	if timestamp != 0 {
		timestamp++
	}
	if seqNum != 0 {
		seqNum++
	}

	startId := fmt.Sprintf("%d-%d", timestamp, seqNum)

	if shouldBlock {
		if blockDelayMs > 0 {
			time.Sleep(time.Duration(blockDelayMs) * time.Millisecond)
		} else {
			xreadBlockMutex.Lock()
			defer xreadBlockMutex.Unlock()
			xreadBlockSignal.Wait()
		}
	}

	streamRespArrs := []string{}
	for i, stream := range streams {
		validEntries, err := getEntriesInRange(*stream, startId, "+")
		if err != nil {
			return "", err
		}

		if shouldBlock && len(validEntries) == 0 {
			continue
		}

		innerRespArrs := []string{}
		for _, entry := range validEntries {
			entryId := fmt.Sprintf("%d-%d", entry.timestamp, entry.sequenceNumber)
			respParts := []string{}
			for key, value := range entry.values {
				respParts = append(respParts, key, value)
			}

			respStr := "*2\r\n" + toRespStr(entryId) + toRespArr(respParts...)
			innerRespArrs = append(innerRespArrs, respStr)
		}

		streamRespStr := "*2\r\n" + toRespStr(streamIds[i]) + fmt.Sprintf("*%d\r\n", len(innerRespArrs)) + strings.Join(innerRespArrs, "")
		streamRespArrs = append(streamRespArrs, streamRespStr)
	}

	response := fmt.Sprintf("*%d\r\n", len(streams)) + strings.Join(streamRespArrs, "")
	if shouldBlock && len(streamRespArrs) == 0 {
		response = "$-1\r\n"
	}

	return response, nil
}

func incrCommand(args []string, client *Client) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("error performing incr: no args")
	}

	key := args[0]
	item, exists := keyValueCache[key]
	if !exists {
		keyValueCache[key] = CacheItem{
			value:     "1",
			expiresAt: -1,
			itemType:  "string",
		}
		return ":1\r\n", nil
	}

	numberVal, err := strconv.Atoi(item.value)
	if err != nil {
		return incrNotNumErr, nil
	}

	item.value = strconv.Itoa(numberVal + 1)
	keyValueCache[key] = item

	return fmt.Sprintf(":%d\r\n", numberVal+1), nil
}

func multiCommand(args []string, client *Client) (string, error) {
	client.queueFlag = true

	return "+OK\r\n", nil
}

func execCommand(args []string, client *Client) (string, error) {
	defer func() {
		client.commandQueue = [][]string{}
		client.queueFlag = false
	}()

	if !client.queueFlag {
		return execNotInQueueModeErr, nil
	}

	if len(client.commandQueue) == 0 {
		return "*0\r\n", nil
	}

	client.queueFlag = false
	responses := []string{}
	for _, command := range client.commandQueue {
		queuedArgs := command[1:]
		if command[1] != "exec" {
			response, _ := runCommand(command[0], queuedArgs, client)
			responses = append(responses, response)
		}
	}

	response := fmt.Sprintf("*%d\r\n", len(responses)) + strings.Join(responses, "")
	return response, nil
}

func discardCommand(args []string, client *Client) (string, error) {
	if !client.queueFlag {
		return discardNotInQueueModeErr, nil
	}

	client.commandQueue = [][]string{}
	client.queueFlag = false

	return "+OK\r\n", nil
}

func runCommand(commandName string, args []string, client *Client) (string, error) {
	commandHandler, exists := commands[commandName]
	if !exists {
		fmt.Printf("Error running command '%s': command does not exist\n", commandName)
	}

	fmt.Printf("%s running command: %s %v\n", configParams["role"], commandName, args)

	response, err := commandHandler(args, client)
	if err != nil {
		return "-ERR\r\n", err
	}

	return response, err
}

func forwardCommandToReplicas(command string) {
	replicasLock.Lock()
	defer replicasLock.Unlock()

	for _, replica := range replicas {
		go func() {
			if _, innerErr := replica.Write([]byte(command)); innerErr != nil {
				fmt.Println("Failed to relay command to replica", innerErr.Error())
				return
			}
		}()
	}
}
