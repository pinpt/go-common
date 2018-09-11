package main

import (
	"crypto/rand"
)

var LatinAndNumbers = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func random(count int, chars []byte) string {
	ra := make([]byte, count)
	_, err := rand.Read(ra)
	if err != nil {
		panic(err)
	}
	res := make([]byte, count)
	lenc := byte(len(chars))
	for i, b := range ra {
		res[i] = chars[b%lenc]
	}
	return string(res)
}
