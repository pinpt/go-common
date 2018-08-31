package json

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/pinpt/go-common/fileutil"
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

// ReadFile Tries to open a file from the file system and read it into an object
func ReadFile(f string, out interface{}) error {
	if !fileutil.FileExists(f) {
		return fmt.Errorf("File not found %v", f)
	}
	js, err := os.Open(f)
	defer js.Close()
	if err != nil {
		return err
	}
	return json.NewDecoder(js).Decode(&out)
}
