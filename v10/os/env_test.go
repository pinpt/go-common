package os

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func randomEnvName() string {
	c := 5
	b := make([]byte, c)
	rand.Read(b)
	return fmt.Sprintf("ENV_TEST_%x", b)
}
func TestGetenvInt(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	name := randomEnvName()
	defer os.Setenv(name, "")
	os.Setenv(name, "123")
	assert.Equal(123, GetenvInt(name, 0))
	assert.Equal(0, GetenvInt("foobar"+name, 0))
	assert.Equal(999, GetenvInt("foobar"+name, 999))
}

func TestGetenv(t *testing.T) {
	assert := assert.New(t)
	n := randomEnvName()
	defer os.Unsetenv(n)
	v := Getenv(n, "a")
	assert.Equal(v, "a")
	os.Setenv(n, "b")
	assert.Equal(Getenv(n, "a"), "b")
}
