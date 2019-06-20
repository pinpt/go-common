package events

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostEvent(t *testing.T) {

	if os.Getenv("CI") == "" {
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

}
func TestGetCSRFToken(t *testing.T) {

	if os.Getenv("CI") == "" {

		t.Parallel()

		assert := assert.New(t)

		ctx := context.Background()

		channel := "dev"

		apiKey := "APIKEY"

		csrfToken, err := getCSRFToken(ctx, channel, apiKey)

		assert.NoError(err)
		assert.IsType("string", csrfToken)
	}
}
