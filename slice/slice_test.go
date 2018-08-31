package slice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveStringDups(t *testing.T) {
	assert := assert.New(t)

	array := []string{"hello", "world", "hello", "world", "pinpt", "pinpt"}
	array = RemoveStringDups(array)

	shouldBe := []string{"hello", "world", "pinpt"}
	assert.ElementsMatch(array, shouldBe)
}
