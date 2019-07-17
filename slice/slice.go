package slice

// RemoveStringDups returns a new slice without duplicate items
func RemoveStringDups(intSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

// ConvertToStringToInterface recursively converts map[interface{}]interface to map[string]interface{}.
// This is for marshalling to json, and exists because yaml unmarshals objects as map[interface{}]interface{}
func ConvertToStringToInterface(badMap map[interface{}]interface{}) map[string]interface{} {
	strInt := make(map[string]interface{})
	for k, v := range badMap {
		if str, ok := k.(string); ok {
			if newBadMap, ok := v.(map[interface{}]interface{}); ok {
				strInt[str] = ConvertToStringToInterface(newBadMap)
			} else if arr, ok := v.([]interface{}); ok {
				fixedArray := make([]interface{}, 0)
				for _, item := range arr {
					if newBadMap, ok := item.(map[interface{}]interface{}); ok {
						fixedArray = append(fixedArray, ConvertToStringToInterface(newBadMap))
					} else {
						fixedArray = append(fixedArray, item)
					}
				}
				strInt[str] = fixedArray
			} else if arrmap, ok := v.([]map[interface{}]interface{}); ok {
				fixedArray := make([]interface{}, 0)
				for _, item := range arrmap {
					fixedArray = append(fixedArray, ConvertToStringToInterface(item))
				}
				strInt[str] = fixedArray
			} else {
				strInt[str] = v
			}
		}
	}
	return strInt
}
