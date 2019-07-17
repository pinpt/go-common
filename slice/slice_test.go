package slice

import (
	"encoding/json"
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

type color struct {
	R int
	G int
	B int
}

type pet struct {
	Name     string
	FurColor color
}

type person struct {
	Name string
	Pets []pet
}

type obj struct {
	Person person
}

func TestConvertToStringToInterfaces(t *testing.T) {
	assert := assert.New(t)
	thing := map[interface{}]interface{}{
		"person": map[interface{}]interface{}{
			"name": "Jeff",
			"pets": []map[interface{}]interface{}{
				map[interface{}]interface{}{
					"name": "Sadie",
					"furColor": map[interface{}]interface{}{
						"r": 0,
						"g": 0,
						"b": 0,
					},
				},
			},
		},
	}
	var abstractThing map[interface{}]interface{}
	abstractThing = thing
	betterThing := ConvertToStringToInterface(abstractThing)
	buf, err := json.Marshal(betterThing)
	assert.NoError(err)
	var jthing obj
	err = json.Unmarshal(buf, &jthing)
	assert.NoError(err)
	assert.Equal("Jeff", jthing.Person.Name)
	assert.Equal("Sadie", jthing.Person.Pets[0].Name)
	assert.Equal(0, jthing.Person.Pets[0].FurColor.R)
	assert.Equal(0, jthing.Person.Pets[0].FurColor.G)
	assert.Equal(0, jthing.Person.Pets[0].FurColor.B)
}
