package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type CacheItem struct {
	value     string
	expiresAt int64
}

var cache = map[string]CacheItem{}
var configParams = map[string]string{}

func main() {
	dirFlag := flag.String("dir", "", "")
	dbFilenameFlag := flag.String("dbfilename", "", "")

	flag.Parse()

	configParams["dir"] = *dirFlag
	configParams["dbfilename"] = *dbFilenameFlag

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
