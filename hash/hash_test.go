package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Checks that NewStream with .gz does not leak open files. This returns too many open files error if NewStream is not using writerWrapper
func TestHashNil(t *testing.T) {
	assert := assert.New(t)
	hashFunc := func() {
		Values(nil)
	}
	assert.NotPanics(hashFunc)
}
