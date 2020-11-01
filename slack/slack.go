package slack

import (
	"context"
	"errors"
	"fmt"

	"github.com/nlopes/slack"
)

// Client Slack bot client.
// needed scopes: chat:write, channels:read, groups:read, im:read, mpim:read
type Client interface {
	SendMessage(msg string) error
}

// New creates a new instance. Needs the bot token and the channel name
func New(token string, channel string) (Client, error) {
	slackClient := slack.New(token)
	if _, err := slackClient.AuthTest(); err != nil {
		return nil, err
	}
	var slackChannelID string
	var cursor string
	for {
		var err error
		var channels []slack.Channel
		// needed scopes:
		channels, cursor, err = slackClient.GetConversations(&slack.GetConversationsParameters{
			ExcludeArchived: "true",
			Limit:           1000,
			Types:           []string{"public_channel", "private_channel"},
			Cursor:          cursor,
		})
		if err != nil {
			return nil, fmt.Errorf("error getting channel ids from slack. error: %v", err)
		}
		if len(channels) == 0 {
			return nil, fmt.Errorf("error getting channel ids from slack")
		}

		for _, c := range channels {
			if c.Name == channel {
				slackChannelID = c.ID
				break
			}
		}
		if slackChannelID != "" {
			break
		}
	}
	if slackChannelID == "" {
		return nil, fmt.Errorf("error finding slack channel %s", channel)
	}
	return &client{
		slackChannelID: slackChannelID,
		slackClient:    slackClient,
	}, nil
}

type client struct {
	slackChannelID string
	slackClient    *slack.Client
}

// SendMessage sends a message to thr slack channel
func (c *client) SendMessage(msg string) error {
	if c.slackChannelID == "" {
		return errors.New("no channel found")
	}
	if _, _, err := c.slackClient.PostMessageContext(context.Background(),
		c.slackChannelID,
		slack.MsgOptionText(msg, false),
		slack.MsgOptionAsUser(false),
	); err != nil {
		return err
	}
	return nil
}
