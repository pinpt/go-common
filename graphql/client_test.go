package graphql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pinpt/go-common/hash"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)

	apikey := os.Getenv("PP_AUTH_SHARED_SECRET")
	hostname := os.Getenv("PP_GRAPHQL_URL")

	// if hostname is missing, use the mock server
	if hostname == "" {
		var closer func()
		closer, hostname = mockServer()
		defer closer()
	}
	client, err := NewClient(hash.Values("customer 0"), "", apikey, hostname)
	assert.NoError(err)

	id, err := createNewCustomer(client, "Dummy Customer", "localhost")

	assert.NoError(err)
	assert.NotEmpty(id)

	deleted, err := deleteCustomer(client, id)
	assert.NoError(err)
	assert.True(deleted)
}

func mockServer() (closer func(), url string) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var payload struct {
			Variables Variables `json:"variables"`
			Query     string    `json:"query"`
		}

		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		bodyString := strings.TrimSpace(payload.Query)
		if strings.HasPrefix(bodyString, "mutation createNewCustomer") {
			fmt.Fprintf(w, `{"data":{"admin":{"createCustomer":{"_id":"437384b8bacc1653"}}}}`)
			return
		}
		if strings.HasPrefix(bodyString, "mutation deleteCustomer") {
			fmt.Fprintf(w, `{"data":{"admin":{"deleteCustomer":true}}}`)
			return
		}
		http.Error(w, fmt.Sprintf("response not handled, body: %s", body), http.StatusBadRequest)
	}))
	return ts.Close, ts.URL
}

func createNewCustomer(client Client, name string, hostname string) (_id string, _ error) {
	var res struct {
		Admin struct {
			CreateCustomer struct {
				ID string `json:"_id"`
			} `json:"createCustomer"`
		} `json:"admin"`
	}
	err := client.Query(`
		mutation createNewCustomer($crmid: String!, $apikey: String!, $name: String!, $hostname: String!) {
			admin {
				createCustomer(input: {
					crm_id: $crmid,
					apikey_id: $apikey,
					name: $name,
					hostname: $hostname
				}) {
					_id
				}
			} 
		} `,
		Variables{
			"apikey":   hash.Values(1, rand.Float64()),
			"crmid":    hash.Values(2, rand.Float64()),
			"name":     name,
			"hostname": hostname,
		}, &res,
	)
	return res.Admin.CreateCustomer.ID, err
}

func deleteCustomer(client Client, customerID string) (bool, error) {
	var res struct {
		Admin struct {
			DeleteCustomer bool `json:"deleteCustomer"`
		} `json:"admin"`
	}
	err := client.Mutate(`
		mutation deleteCustomer($id: String!) {
			admin {
				deleteCustomer(_id: $id)
			}
		}`,
		Variables{
			"id": customerID,
		}, &res,
	)
	return res.Admin.DeleteCustomer, err
}
