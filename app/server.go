package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
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

type Client struct {
	conn         net.Conn
	queueFlag    bool
	commandQueue [][]string
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

	commands = map[string]func([]string, *Client) (string, error){
		"echo":     echoCommand,
		"ping":     pingCommand,
		"set":      setCommand,
		"get":      getCommand,
		"config":   configCommand,
		"keys":     keysCommand,
		"info":     infoCommand,
		"replconf": replconfCommand,
		"psync":    psyncCommand,
		"wait":     waitCommand,
		"type":     typeCommand,
		"xadd":     xaddCommand,
		"xrange":   xrangeCommand,
		"xread":    xreadCommand,
		"incr":     incrCommand,
		"multi":    multiCommand,
		"exec":     execCommand,
		"discard":  discardCommand,
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+configParams["port"])
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", configParams["port"])
		os.Exit(1)
	}

	go listenForAndHandleClientConnections(listener)

	if configParams["role"] == "slave" {
		go connectToMaster()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	fmt.Println("Shutting down gracefully...")
	listener.Close()

	os.Exit(0)
}

func listenForAndHandleClientConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		reader := bufio.NewReader(conn)

		client := Client{
			conn:         conn,
			queueFlag:    false,
			commandQueue: [][]string{},
		}
		go handleClient(&client, reader)
	}
}

func handleClient(client *Client, reader *bufio.Reader) {
	defer client.conn.Close()

	for {
		rawCommand, commandName, args, err := parseRespCommand(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from connection: ", err.Error())
			}
			break
		}

		shouldQueueCommand := client.queueFlag && commandName != "exec" && commandName != "discard"
		if shouldQueueCommand {
			fmt.Printf("Queueing command: %s\n", commandName)
			client.commandQueue = append(client.commandQueue, append([]string{commandName}, args...))
			if _, err := client.conn.Write([]byte("+QUEUED\r\n")); err != nil {
				fmt.Println("Error responding after queueing command: ", err.Error())
				break
			}
			continue
		}

		response, err := runCommand(commandName, args, client)
		if err != nil {
			fmt.Printf("Error performing command %s: %s\n", commandName, err.Error())
		} else if commandName == "set" {
			fmt.Printf("Forwarding %s to replicas\n", commandName)
			forwardCommandToReplicas(rawCommand)
		}

		bytesProcessed += len(rawCommand)

		shouldSendResponse := len(response) > 0 && (configParams["role"] == "master" ||
			commandName == "replconf" ||
			commandName == "get" ||
			commandName == "info")
		if shouldSendResponse {
			if _, err := client.conn.Write([]byte(response)); err != nil {
				fmt.Println("Error sending command response:", err.Error())
				break
			}
		}
	}

	fmt.Println("Closing client connection")
}
