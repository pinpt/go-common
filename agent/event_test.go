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

	headers := make(map[string]string)

	err := PostEvent(ctx, event, channel, apiKey, headers)

	assert.NoError(err)
}
func TestGetCSRFToken(t *testing.T) {

	t.Parallel()

	assert := assert.New(t)

	ctx := context.Background()

	channel := "dev"

	apiKey := "APIKEY"

	csrfToken, err := getCSRFToken(ctx, channel, apiKey)

	assert.NoError(err)
	assert.IsType("string", csrfToken)
}
