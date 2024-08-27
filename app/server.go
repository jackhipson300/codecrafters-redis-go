package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type CacheItem struct {
	value     string
	expiresAt int64
	itemType  string
}

type StreamEntry struct {
	timestamp      int64
	sequenceNumber int
	values         map[string]string
}

type Stream struct {
	lastMillisecondsTime int64
	lastSequenceNumber   int
	entries              []StreamEntry
}

var keyValueCache = map[string]CacheItem{}
var streamCache = map[string]*Stream{}
var configParams = map[string]string{}

var replicas = []net.Conn{}
var replicasLock = sync.Mutex{}

var bytesProcessed = 0
var setHasOccurred = false

var numAcksSinceLasSet = 0
var ackLock = sync.Mutex{}

func main() {
	dirFlag := flag.String("dir", "", "")
	dbFilenameFlag := flag.String("dbfilename", "", "")
	portFlag := flag.String("port", "", "")
	replicaofFlag := flag.String("replicaof", "", "")

	flag.Parse()

	configParams["dir"] = *dirFlag
	configParams["dbfilename"] = *dbFilenameFlag
	configParams["port"] = *portFlag
	configParams["role"] = "master"
	configParams["master"] = *replicaofFlag

	if configParams["port"] == "" {
		configParams["port"] = "6379"
	}
	if *replicaofFlag != "" {
		configParams["role"] = "slave"
	}

	if configParams["role"] == "master" {
		configParams["replId"] = generateReplId()
		configParams["replOffset"] = "0"
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+configParams["port"])
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", configParams["port"])
		os.Exit(1)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			reader := bufio.NewReader(conn)

			go handleClient(conn, reader)
		}
	}()

	if configParams["role"] == "slave" {
		parts := strings.Split(configParams["master"], " ")
		conn, err := net.Dial("tcp", parts[0]+":"+parts[1])
		if err != nil {
			fmt.Printf("Failed to connect to master (%s:%s)\n", parts[0], parts[1])
			os.Exit(1)
		}
		fmt.Printf("Connected to master (%s:%s)\n", parts[0], parts[1])

		reader := bufio.NewReader(conn)

		go handleMaster(conn, reader)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	fmt.Println("Shutting down gracefully...")
	l.Close()

	os.Exit(0)
}

func handleClient(conn net.Conn, reader *bufio.Reader) {
	defer conn.Close()

	numArgsLeft := 0
	command := ""
	rawCommand := ""
	args := []string{}
	for {
		part, err := readResp(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from connection (will close): ", err.Error())
			}
			return
		}

		rawCommand += part + "\r\n"
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
			runCommand(rawCommand, command, args, conn)

			command = ""
			rawCommand = ""
			args = []string{}
		}
	}
}
