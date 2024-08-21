package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type CacheItem struct {
	value     string
	expiresAt int64
}

var cache = map[string]CacheItem{}

const nullRespStr = "$-1\r\n"

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn)
	}
}

func toRespStr(raw string) string {
	length := len(raw)
	return fmt.Sprintf("$%d\r\n%s\r\n", length, raw)
}

func echo(args []string, conn net.Conn) {
	if len(args) == 0 {
		fmt.Println("Error performing echo: no args")
		return
	}

	if _, err := conn.Write([]byte(toRespStr(args[0]))); err != nil {
		fmt.Println("Error performing echo: ", err.Error())
	}
}

func ping(args []string, conn net.Conn) {
	if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
		fmt.Println("Error performing png: ", err.Error())
	}
}

func set(args []string, conn net.Conn) {
	now := time.Now()

	if len(args) < 2 {
		fmt.Println("Error performing set: not enough args")
		return
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
		fmt.Println("Error performing set: ", err.Error())
	}
}

func get(args []string, conn net.Conn) {
	now := time.Now().UnixMilli()

	if len(args) == 0 {
		fmt.Println("Error performing get: no args")
		return
	}

	entry, exists := cache[args[0]]
	if !exists {
		if _, err := conn.Write([]byte(nullRespStr)); err != nil {
			fmt.Println("Error performing get: ", err.Error())
		}
		return
	}

	writeVal := toRespStr(entry.value)
	if entry.expiresAt != -1 && now >= entry.expiresAt {
		fmt.Println("Entry expired: ", now, entry.expiresAt)
		writeVal = nullRespStr
	}

	if _, err := conn.Write([]byte(writeVal)); err != nil {
		fmt.Println("Error performing get: ", err.Error())
	}
}

var commands = map[string]func([]string, net.Conn){
	"echo": echo,
	"ping": ping,
	"set":  set,
	"get":  get,
}

func runCommand(commandName string, args []string, conn net.Conn) {
	command, exists := commands[commandName]
	if !exists {
		fmt.Printf("Error running command '%s': command does not exist\n", commandName)
	}

	fmt.Println("Running command: ", commandName, args)

	command(args, conn)
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	commandParts := make(chan string)
	go func() {
		defer close(commandParts)
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection (will close): ", err.Error())
				return
			}

			if len(message) < 3 {
				fmt.Println("Error reading from connection (will close): message too short")
				return
			}

			message = message[:len(message)-2]
			commandParts <- message
		}
	}()

	numArgsLeft := 0
	command := ""
	args := []string{}
	for part := range commandParts {
		if numArgsLeft == 0 && (part[0] != '*' || len(part) == 1) {
			continue
		}

		if numArgsLeft == 0 {
			numArgsLeft, _ = strconv.Atoi(part[1:])
			continue
		}

		switch part[0] {
		case '$':
			continue
		default:
			if len(command) == 0 {
				command = strings.ToLower(part)
			} else {
				args = append(args, part)
			}
			numArgsLeft--
		}

		if numArgsLeft == 0 {
			runCommand(command, args, conn)
			command = ""
			args = []string{}
		}
	}
}
