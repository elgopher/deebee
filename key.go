package deebee

import (
	"fmt"
	"strings"
)

func validateKey(key string) error {
	if strings.HasPrefix(key, " ") {
		return newClientError(fmt.Sprintf("invalid key: starts with space: \"%s\"", key))
	}
	if strings.HasSuffix(key, " ") {
		return newClientError(fmt.Sprintf("invalid key: ends with space: \"%s\"", key))
	}
	if key == "" || key == "." || key == ".." || strings.Contains(key, "/") || strings.Contains(key, "\\") {
		return newClientError(fmt.Sprintf("invalid key: \"%s\"", key))
	}
	return nil
}
