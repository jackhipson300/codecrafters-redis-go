package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var mutex = sync.Mutex{}

type ValueInfo struct {
	position int
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

func getKeys() (map[string]ValueInfo, error) {
	if configParams["dir"] == "" || configParams["dbfilename"] == "" {
		fmt.Println("error getting keys: directory or filename not in config")
		return make(map[string]ValueInfo), nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	contents, err := os.ReadFile(configParams["dir"] + "/" + configParams["dbfilename"])
	if err != nil {
		return nil, fmt.Errorf("error getting keys: %w", err)
	}

	dbStartIdx := -1
	for i, b := range contents {
		if b == 0xFE {
			dbStartIdx = i
			break
		}
	}

	if dbStartIdx == -1 {
		return nil, fmt.Errorf("error getting keys: no db section found")
	}

	keys := make(map[string]ValueInfo)
	for idx := dbStartIdx + 5; idx < len(contents) && contents[idx] != 0xFF; idx++ {
		if contents[idx] == 0xFC {
			idx += 9
		}
		if contents[idx] == 0xFD {
			idx += 5
		}
		if contents[idx] != 0x00 {
			continue
		}
		idx += 1

		key, n := decodeString(contents, idx)
		idx += n

		keys[key] = ValueInfo{position: n}
	}

	return keys, nil
}

func getValue(valueInfo ValueInfo) (string, error) {
	mutex.Lock()
	defer mutex.Unlock()

	contents, err := os.ReadFile(configParams["dir"] + "/" + configParams["dbfilename"])
	if err != nil {
		return "", fmt.Errorf("error getting keys: %w", err)
	}

	value, _ := decodeString(contents, valueInfo.position)

	return value, nil
}
