package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const emptyRdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

const xaddEntryIdOlderThanLastErr = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
const xaddEntryIdZeroErr = "-ERR The ID specified in XADD must be greater than 0-0\r\n"

func echoCommand(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing echo: no args")
	}

	if _, err := write(conn, []byte(toRespStr(args[0]))); err != nil {
		return fmt.Errorf("error performing echo: %w", err)
	}

	return nil
}

func pingCommand(args []string, conn net.Conn) error {
	if _, err := write(conn, []byte("+PONG\r\n")); err != nil {
		return fmt.Errorf("error performing png: %w", err)
	}

	return nil
}

func setCommand(args []string, conn net.Conn) error {
	now := time.Now()

	setHasOccurred = true

	ackLock.Lock()
	numAcksSinceLasSet = 0
	ackLock.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("error performing set: not enough args")
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
	if _, err := write(conn, []byte("+OK\r\n")); err != nil {
		return fmt.Errorf("error performing set: %w", err)
	}

	return nil
}

func getCommand(args []string, conn net.Conn) error {
	now := time.Now().UnixMilli()

	if len(args) == 0 {
		return fmt.Errorf("error performing get: no args")
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
			return fmt.Errorf("error performing get: %w", err)
		}

		valueInfo, exists := keysMap[args[0]]
		if exists {
			expiresAt = valueInfo.expiresAt
			value, err = getValue(valueInfo)
			if err != nil {
				return fmt.Errorf("error performing get: %w", err)
			}
		}
	}

	if value == "" {
		if _, err := conn.Write([]byte(nullRespStr)); err != nil {
			return fmt.Errorf("error performing get: %w", err)
		}
		return fmt.Errorf("error performing get: entry does not exist")
	}

	writeVal := toRespStr(value)
	if expiresAt != -1 && now >= expiresAt {
		fmt.Println("Entry expired: ", now, entry.expiresAt)
		writeVal = nullRespStr
	}

	if _, err := conn.Write([]byte(writeVal)); err != nil {
		return fmt.Errorf("error performing get: %w", err)
	}

	return nil
}

func configCommand(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing config: no args")
	}

	switch strings.ToLower(args[0]) {
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("error performing config get: not enough args")
		}

		response := nullRespStr
		value, exists := configParams[args[1]]
		if exists && value != "" {
			response = fmt.Sprintf("*2\r\n%s%s", toRespStr(args[1]), toRespStr(value))
		}

		if _, err := write(conn, []byte(response)); err != nil {
			return fmt.Errorf("error performing config get: %w", err)
		}
	}

	return nil
}

func keysCommand(args []string, conn net.Conn) error {
	keysMap, err := getKeys()
	if err != nil {
		return err
	}

	response := fmt.Sprintf("*%d\r\n", len(keysMap))
	for key := range keysMap {
		response += toRespStr(key)
	}

	if _, err := write(conn, []byte(response)); err != nil {
		return fmt.Errorf("error performing keys: %w", err)
	}

	return nil
}

func infoCommand(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing info: no args")
	}

	if args[0] == "replication" {
		response := "role:" + configParams["role"]
		if configParams["role"] == "master" {
			addToInfoResponse("master_replid", configParams["replId"], &response)
			addToInfoResponse("master_repl_offset", configParams["replOffset"], &response)
		}

		if _, err := conn.Write([]byte(toRespStr(response))); err != nil {
			return fmt.Errorf("error performing info: %w", err)
		}
	}

	return nil
}

func replconfCommand(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing replconf: no args")
	}

	switch strings.ToLower(args[0]) {
	case "listening-port":
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	case "capa":
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	case "getack":
		if _, err := conn.Write([]byte(toRespArr("REPLCONF", "ACK", strconv.Itoa(bytesProcessed)))); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	case "ack":
		if len(args) < 2 {
			return fmt.Errorf("error handling replconf ack: not enough args")
		}

		ackLock.Lock()
		numAcksSinceLasSet++
		ackLock.Unlock()
	}

	return nil
}

func psyncCommand(args []string, conn net.Conn) error {
	if len(args) < 2 {
		return fmt.Errorf("error performing psync: not enough args")
	}

	if args[0] == "?" {
		response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", configParams["replId"])
		if _, err := write(conn, []byte(response)); err != nil {
			return fmt.Errorf("error performing psync: %w", err)
		}

		emptyRdbAsBytes, _ := hex.DecodeString(emptyRdb)
		fileResponse := append([]byte(fmt.Sprintf("$%d\r\n", len(emptyRdbAsBytes))), emptyRdbAsBytes...)
		if _, err := write(conn, fileResponse); err != nil {
			return fmt.Errorf("error performing psync: %w", err)
		}

		replicasLock.Lock()
		defer replicasLock.Unlock()

		replicas = append(replicas, conn)
	}

	return nil
}

func waitCommand(args []string, conn net.Conn) error {
	if len(args) < 2 {
		return fmt.Errorf("error performing wait: not enough args")
	}

	if !setHasOccurred {
		fmt.Println("Set has not occurred, sending 0")
		replicasLock.Lock()
		defer replicasLock.Unlock()

		numAcks := len(replicas)

		response := fmt.Sprintf(":%d\r\n", numAcks)
		if _, err := conn.Write([]byte(response)); err != nil {
			return fmt.Errorf("error performing wait: %w", err)
		}

		return nil
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
		return fmt.Errorf("error performing wait: %w", err)
	}
	timeoutMS, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("error performing wait: %w", err)
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
				if _, err := conn.Write([]byte(response)); err != nil {
					return fmt.Errorf("error performing wait: %w", err)
				}
				return nil
			}
		case <-timeoutChannel:
			response := fmt.Sprintf(":%d\r\n", numAcksSinceLasSet)
			if _, err := conn.Write([]byte(response)); err != nil {
				return fmt.Errorf("error performing wait: %w", err)
			}
			return nil
		}
	}
}

func typeCommand(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing type command: no args")
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
				return fmt.Errorf("error performing type command: %w", err)
			}
			_, exists = keys[args[0]]
			if exists {
				itemType = "string"
			}
		}
	}

	if itemType != "" {
		if _, err := write(conn, []byte(fmt.Sprintf("+%s\r\n", itemType))); err != nil {
			return fmt.Errorf("error performing type command: %w", err)
		}
	} else {
		if _, err := write(conn, []byte("+none\r\n")); err != nil {
			return fmt.Errorf("error performing type command: %w", err)
		}
	}

	return nil
}

func xaddCommand(args []string, conn net.Conn) error {
	if len(args) < 4 {
		return fmt.Errorf("error performing xadd: not enough args")
	}

	streamId := args[0]
	entryId := args[1]

	entryIdParts := strings.Split(entryId, "-")
	if len(entryIdParts) < 2 {
		return fmt.Errorf("error performing xadd: invalid entry id")
	}

	millisecondsTime, err := strconv.Atoi(entryIdParts[0])
	if err != nil {
		return fmt.Errorf("error performing xadd: invalid entry id: %w", err)
	}
	sequenceNumber, err := strconv.Atoi(entryIdParts[1])
	if err != nil {
		return fmt.Errorf("error performing xadd: invalid entry id: %w", err)
	}

	stream, exists := streamCache[streamId]
	if !exists {
		stream = &Stream{}
		stream.entries = map[string]map[string]string{}
		streamCache[streamId] = stream
	}

	if entryId == "0-0" && len(stream.entries) > 0 {
		_, err := write(conn, []byte(xaddEntryIdZeroErr))
		return err
	}

	millisecondsTimeInvalid := stream.lastMillisecondsTime > millisecondsTime
	sequenceNumberInvalid := stream.lastMillisecondsTime == millisecondsTime &&
		stream.lastSequenceNumber >= sequenceNumber
	if millisecondsTimeInvalid || sequenceNumberInvalid {
		_, err := write(conn, []byte(xaddEntryIdOlderThanLastErr))
		return err
	}

	stream.lastMillisecondsTime = millisecondsTime
	stream.lastSequenceNumber = sequenceNumber

	entry, exists := stream.entries[entryId]
	if !exists {
		entry = map[string]string{}
		stream.entries[entryId] = entry
	}

	for i := 2; i < len(args); i += 2 {
		key := args[i]
		value := args[i+1]
		entry[key] = value
	}

	if _, err := write(conn, []byte(toRespStr(entryId))); err != nil {
		return fmt.Errorf("error performing xadd: %w", err)
	}

	return nil
}

type Command struct {
	handler         func([]string, net.Conn) error
	shouldReplicate bool
}

var commands = map[string]Command{
	"echo":     {handler: echoCommand, shouldReplicate: false},
	"ping":     {handler: pingCommand, shouldReplicate: false},
	"set":      {handler: setCommand, shouldReplicate: true},
	"get":      {handler: getCommand, shouldReplicate: false},
	"config":   {handler: configCommand, shouldReplicate: false},
	"keys":     {handler: keysCommand, shouldReplicate: false},
	"info":     {handler: infoCommand, shouldReplicate: false},
	"replconf": {handler: replconfCommand, shouldReplicate: false},
	"psync":    {handler: psyncCommand, shouldReplicate: false},
	"wait":     {handler: waitCommand, shouldReplicate: false},
	"type":     {handler: typeCommand, shouldReplicate: false},
	"xadd":     {handler: xaddCommand, shouldReplicate: false},
}

func runCommand(rawCommand string, commandName string, args []string, conn net.Conn) {
	command, exists := commands[commandName]
	if !exists {
		fmt.Printf("Error running command '%s': command does not exist\n", commandName)
	}

	fmt.Printf("%s running command: %s %v\n", configParams["role"], commandName, args)

	err := command.handler(args, conn)
	if err != nil {
		fmt.Println("Error running command: ", err.Error())
	}

	bytesProcessed += len(rawCommand)

	if command.shouldReplicate {
		replicasLock.Lock()
		defer replicasLock.Unlock()

		for _, replica := range replicas {
			go func() {
				if _, err := replica.Write([]byte(rawCommand)); err != nil {
					fmt.Println("Failed to relay command to replica", err.Error())
					return
				}
			}()
		}
	}
}
