package main

import "testing"

func BenchmarkRandom(b *testing.B) {
	// 0.1MB
	count := 100 * 1000
	b.SetBytes(int64(count))
	for i := 0; i < b.N; i++ {
		random(count, LatinAndNumbers)
	}
}
