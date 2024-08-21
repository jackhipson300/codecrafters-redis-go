package main

import "fmt"

const nullRespStr = "$-1\r\n"

func toRespStr(raw string) string {
	length := len(raw)
	return fmt.Sprintf("$%d\r\n%s\r\n", length, raw)
}
