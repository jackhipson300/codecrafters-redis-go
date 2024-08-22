package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func echo(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing echo: no args")
	}

	if _, err := conn.Write([]byte(toRespStr(args[0]))); err != nil {
		return fmt.Errorf("error performing echo: %w", err)
	}

	return nil
}

func ping(args []string, conn net.Conn) error {
	if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
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
	if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
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

		if _, err := conn.Write([]byte(response)); err != nil {
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

	if _, err := conn.Write([]byte(response)); err != nil {
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

var commands = map[string]func([]string, net.Conn) error{
	"echo":   echo,
	"ping":   ping,
	"set":    set,
	"get":    get,
	"config": config,
	"keys":   keys,
	"info":   info,
}

func runCommand(commandName string, args []string, conn net.Conn) {
	command, exists := commands[commandName]
	if !exists {
		fmt.Printf("Error running command '%s': command does not exist\n", commandName)
	}

	fmt.Println("Running command: ", commandName, args)

	err := command(args, conn)
	if err != nil {
		fmt.Println("Error running command: ", err.Error())
	}
}
