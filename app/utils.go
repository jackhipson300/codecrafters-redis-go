package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

const nullRespStr = "$-1\r\n"

func toRespStr(raw string) string {
	length := len(raw)
	return fmt.Sprintf("$%d\r\n%s\r\n", length, raw)
}

func generateReplId() string {
	bytes := make([]byte, 30)
	rand.Read(bytes)

	return base64.URLEncoding.EncodeToString(bytes)[:40]
}

func addToInfoResponse(key string, value string, response *string) {
	*response += "\r\n" + key + ":" + value
}
