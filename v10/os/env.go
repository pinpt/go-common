package os

import (
	"os"
	"strconv"
)

// Getenv will return an environment variable if exists or default if not
func Getenv(name, def string) string {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	return v
}

// GetenvInt returns an environment variable as a number
func GetenvInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
