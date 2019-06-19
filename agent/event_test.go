package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostEvent(t *testing.T) {

	t.Parallel()

	assert := assert.New(t)
	ctx := context.Background()

	event := Event{
		CustomerID: "CUSTOMER",
		Type:       "export",
		UUID:       "UUID",
		OS:         "os",
		Distro:     "distro",
		Version:    "version",
		Hostname:   "hostname",
		// more fields
	}

	channel := "dev"

	apiKey := "APIKEY"

	csrfToken := "5q4c7jsg55FZuk2pwvL4JFF+f5dWPqg5eIo0koUmTjYOdSwesKm4h93cB6LKn7LyBuSkas8Fabva1lTbXi7K/g=="

	headers := make(map[string]string)

	err := PostEvent(ctx, event, channel, apiKey, csrfToken, headers)

	assert.NoError(err)
}
func TestGetCSRFToken(t *testing.T) {

	t.Parallel()

	assert := assert.New(t)

	ctx := context.Background()

	channel := "dev"

	apiKey := "APIKEY"

	csrfToken, err := GetCSRFToken(ctx, channel, apiKey)

	assert.NoError(err)
	assert.IsType("string", csrfToken)
}
