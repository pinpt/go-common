package graphql

import (
	pjson "github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/log"
)

// debugGraphQLClient logs everything it does for debugging
type debugGraphQLClient struct {
	client Client
	logger log.Logger
}

var _ Client = (*debugGraphQLClient)(nil)

func (c debugGraphQLClient) Query(query string, vars Variables, out interface{}) error {
	log.Debug(c.logger, "query", "vars", pjson.Stringify(vars), "query", query)
	return c.client.Query(query, vars, out)
}

func (c debugGraphQLClient) Mutate(query string, vars Variables, out interface{}) error {
	log.Debug(c.logger, "mutate", "vars", pjson.Stringify(vars), "query", query)
	return c.client.Mutate(query, vars, out)
}

func (c debugGraphQLClient) SetHeader(k, v string) {
	log.Debug(c.logger, "setting header", "key", k, "value", v)
	c.client.SetHeader(k, v)
	return
}

// NewDebugGraphQLClient returns a client that logs everything it does
func NewDebugGraphQLClient(logger log.Logger, client Client) Client {
	return &debugGraphQLClient{
		client: client,
		logger: logger,
	}
}
