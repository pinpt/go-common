package graphql

import (
	"github.com/pinpt/go-common/hash"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestClient(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)

	// these need to be set
	apikey := os.Getenv("PP_AUTH_SHARED_SECRET")
	hostname := os.Getenv("PP_GRAPHQL_URL")

	client, err := NewClient(hash.Values("customer 0"), "", apikey, hostname)
	assert.NoError(err)

	id, err := createNewCustomer(client, "Dummy Customer", "localhost")
	assert.NoError(err)
	assert.NotEmpty(id)

	deleted, err := deleteCustomer(client, id)
	assert.NoError(err)
	assert.True(deleted)
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
