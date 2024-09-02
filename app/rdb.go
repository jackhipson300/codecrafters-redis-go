package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var rdbFile = []byte{}
var rdbMutex = sync.Mutex{}

type ValueInfo struct {
	position  int
	expiresAt int64
}

func decodeString(raw []byte, start int) (string, int) {
	idx := start
	var size uint32 = 0
	if raw[start]&0xC0 == 0x0 {
		size = uint32(raw[start])
		idx += 1
	} else if raw[start]&0xC0 == 0x40 {
		size = binary.BigEndian.Uint32([]byte{raw[start] & 0x3F, raw[start+1]})
		idx += 2
	} else if raw[start]&0xC0 == 0x80 {
		size = binary.BigEndian.Uint32(raw[start+1 : start+5])
		idx += 5
	}

	end := idx + int(size)
	return string(raw[idx:end]), end
}

func initRdbFile() error {
	rdbMutex.Lock()
	defer rdbMutex.Unlock()

	if len(rdbFile) == 0 {
		if configParams["dir"] == "" || configParams["dbfilename"] == "" {
			fmt.Println("error getting keys: directory or filename not in config")
			return nil
		}

		var err error
		contents, err := os.ReadFile(configParams["dir"] + "/" + configParams["dbfilename"])
		if err != nil {
			return fmt.Errorf("error initializing rdb file: %w", err)
		}

		rdbFile = contents
	}

	return nil
}

func getKeys() (map[string]ValueInfo, error) {
	if err := initRdbFile(); err != nil {
		return make(map[string]ValueInfo), fmt.Errorf("error getting keys: %w", err)
	}

	rdbMutex.Lock()
	defer rdbMutex.Unlock()

	dbStartIdx := -1
	for i, b := range rdbFile {
		if b == 0xFE {
			dbStartIdx = i
			break
		}
	}

	if dbStartIdx == -1 {
		return nil, fmt.Errorf("error getting keys: no db section found")
	}

	keys := make(map[string]ValueInfo)
	for idx := dbStartIdx + 5; idx < len(rdbFile) && rdbFile[idx] != 0xFF; {
		expiry := int64(-1)
		if rdbFile[idx] == 0xFC {
			expiry = int64(binary.LittleEndian.Uint64(rdbFile[idx+1 : idx+9]))
			idx += 9
		}
		if rdbFile[idx] == 0xFD {
			expiry = int64(binary.LittleEndian.Uint64(rdbFile[idx+1:idx+5])) * 1000
			idx += 5
		}
		if rdbFile[idx] != 0x00 {
			idx += 1
			continue
		}
		idx += 1

		key, n := decodeString(rdbFile, idx)
		idx = n

		keys[key] = ValueInfo{position: n, expiresAt: expiry}
	}

	return keys, nil
}

func getValue(valueInfo ValueInfo) (string, error) {
	if err := initRdbFile(); err != nil {
		return "", fmt.Errorf("error getting value from rdb: %w", err)
	}

	rdbMutex.Lock()
	defer rdbMutex.Unlock()

	value, _ := decodeString(rdbFile, valueInfo.position)

	return value, nil
}
