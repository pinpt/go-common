package jitter

import (
	"math/rand"
	"time"
)

// GetJitter gets a jitter with a min/max
func GetJitter(min int64, max int64) time.Duration {
	return time.Duration(rand.Int63n(max-min+1) + min)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}