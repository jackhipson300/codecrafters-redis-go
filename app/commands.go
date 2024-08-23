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

func echo(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing echo: no args")
	}

	if _, err := write(conn, []byte(toRespStr(args[0]))); err != nil {
		return fmt.Errorf("error performing echo: %w", err)
	}

	return nil
}

func ping(args []string, conn net.Conn) error {
	if _, err := write(conn, []byte("+PONG\r\n")); err != nil {
		return fmt.Errorf("error performing png: %w", err)
	}

	return nil
}

func set(args []string, conn net.Conn) error {
	now := time.Now()

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

	cache[args[0]] = CacheItem{
		value:     args[1],
		expiresAt: expiresAt,
	}
	if _, err := write(conn, []byte("+OK\r\n")); err != nil {
		return fmt.Errorf("error performing set: %w", err)
	}

	return nil
}

func get(args []string, conn net.Conn) error {
	now := time.Now().UnixMilli()

	if len(args) == 0 {
		return fmt.Errorf("error performing get: no args")
	}

	var value string
	var expiresAt int64
	entry, exists := cache[args[0]]
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

func config(args []string, conn net.Conn) error {
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

func keys(args []string, conn net.Conn) error {
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

func info(args []string, conn net.Conn) error {
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

func replconf(args []string, conn net.Conn) error {
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
	}

	return nil
}

func psync(args []string, conn net.Conn) error {
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

type Command struct {
	handler         func([]string, net.Conn) error
	shouldReplicate bool
}

var commands = map[string]Command{
	"echo":     {handler: echo, shouldReplicate: false},
	"ping":     {handler: ping, shouldReplicate: false},
	"set":      {handler: set, shouldReplicate: true},
	"get":      {handler: get, shouldReplicate: false},
	"config":   {handler: config, shouldReplicate: false},
	"keys":     {handler: keys, shouldReplicate: false},
	"info":     {handler: info, shouldReplicate: false},
	"replconf": {handler: replconf, shouldReplicate: false},
	"psync":    {handler: psync, shouldReplicate: false},
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
		go func() {
			replicasLock.Lock()
			defer replicasLock.Unlock()

			for _, replica := range replicas {
				if _, err := replica.Write([]byte(rawCommand)); err != nil {
					fmt.Println("Failed to relay command to replica")
					continue
				}
			}
		}()
	}
}
