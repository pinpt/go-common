// Modified version of github.com/jeremywohl/flatten

package flatten

import (
	"encoding/json"
	"fmt"
)

// Flatten generates a flat map from a nested one. It will lowecase all keys.
// Slices will be kept as slices, but it's maps will be flatten as well
func Flatten(nested map[string]interface{}) (map[string]interface{}, error) {
	flatmap := make(map[string]interface{})

	err := flatten(true, flatmap, nested, "")
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

// InterfaceToMap converts a raw object into a key-value map pair if possible
func InterfaceToMap(obj interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	rawmap := make(map[string]interface{})
	err = json.Unmarshal(b, &rawmap)
	return rawmap, err
}

func flatten(top bool, flatMap map[string]interface{}, nested interface{}, prefix string) error {
	switch t := nested.(type) {
	case map[string]interface{}:
		for k, v := range nested.(map[string]interface{}) {
			newKey := enkey(top, prefix, k)
			switch val := v.(type) {
			case map[string]interface{}:
				if err := flatten(false, flatMap, v, newKey); err != nil {
					return err
				}
			case []interface{}:
				// stringify objects in arrays
				o := []string{}
				for _, a := range val {
					b, _ := json.Marshal(a)
					o = append(o, string(b))
				}
				flatMap[newKey] = o

			default:
				flatMap[newKey] = v
			}
		}
	default:
		return fmt.Errorf("Not a valid input, %v", t)
	}
	return nil
}

func enkey(top bool, prefix, subkey string) string {
	key := prefix
	if top {
		key += subkey
	} else {
		key += "_" + subkey
	}
	return key
}
