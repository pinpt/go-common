package json

import (
	"encoding/json"
	"fmt"
)

// Stringify will return a JSON formatted string. pass an optional second argument to pretty print
func Stringify(v interface{}, opts ...interface{}) string {
	var buf []byte
	var err error
	if len(opts) > 0 {
		buf, err = json.MarshalIndent(v, "", "\t")
	} else {
		buf, err = json.Marshal(v)
	}
	if err != nil {
		return fmt.Sprintf("<error:%v>", err)
	}
	return string(buf)
}
