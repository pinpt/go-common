package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"

	"github.com/oliveagle/jsonpath"
	"github.com/pinpt/go-common/fileutil"
)

// Deserializer is a callback which will take a json RawMessage for processing
type Deserializer func(line json.RawMessage) error

// Deserialize will return a function which will Deserialize in a flexible way the JSON in reader
func Deserialize(r io.Reader, dser Deserializer) error {
	bufreader := bufio.NewReader(r)
	buf, err := bufreader.Peek(1)
	if err != nil && err != io.EOF {
		return err
	}
	if err == io.EOF && len(buf) == 0 {
		return nil
	}
	dec := json.NewDecoder(bufreader)
	dec.UseNumber()
	token := string(buf[0:1])
	running := true
	for running {
		switch token {
		case "[", "{":
			{
				if token == "[" {
					// advance the array token
					dec.Token()
				}

				for dec.More() {
					var line json.RawMessage
					if err := dec.Decode(&line); err != nil {
						return err
					}
					if err := dser(line); err != nil {
						return err
					}
				}
				// consume the last token
				dec.Token()
				// check to see if we have more data in the buffer in case we
				// have concatenated streams together
				running = dec.More()
			}
		default:
			return fmt.Errorf("invalid json, expected either [ or {")
		}
	}
	return nil
}

// StreamToMap will stream a file into results
func StreamToMap(fp string, keypath string, results map[string]map[string]interface{}, insert bool) error {
	of, err := fileutil.OpenFile(fp)
	if err != nil {
		return fmt.Errorf("error opening: %v. %v", fp, err)
	}
	defer of.Close()
	var keys map[string]bool
	addkeys := len(results) > 0
	if addkeys {
		keys = make(map[string]bool)
		// mark all keys as not found
		for k := range results {
			keys[k] = false
		}
	}
	idfind := keypath == "$.id"
	if err := Deserialize(of, func(buf json.RawMessage) error {
		kv := make(map[string]interface{})
		if err := json.Unmarshal(buf, &kv); err != nil {
			return err
		}
		var key interface{}
		if idfind {
			key = kv["id"]
		} else {
			key, err = jsonpath.JsonPathLookup(kv, keypath)
			if err != nil {
				return err
			}
		}
		var keystr string
		if s, ok := key.(string); ok {
			keystr = s
		} else {
			keystr = fmt.Sprintf("%v", key)
		}
		if insert {
			results[keystr] = kv
			if addkeys {
				keys[keystr] = true
			}
		} else {
			found := results[keystr]
			if found != nil {
				if addkeys {
					keys[keystr] = true
				}
				for k, v := range kv {
					found[k] = v
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// delete any keys not found in the original source (since this is a join)
	for k, v := range keys {
		if !v {
			delete(results, k)
		}
	}
	return nil
}

// JoinTable will join 2 tables from dir into a map of joined maps
func JoinTable(dir string, fromtable string, frompath string, totable string, topath string, kv map[string]interface{}) (map[string]map[string]interface{}, error) {
	results := make(map[string]map[string]interface{})
	fromfile, err := fileutil.FindFiles(dir, regexp.MustCompile(fromtable+"\\.json(\\.gz)?$"))
	if err != nil {
		return nil, err
	}
	tofile, err := fileutil.FindFiles(dir, regexp.MustCompile(totable+"\\.json(\\.gz)?$"))
	if err != nil {
		return nil, err
	}
	if len(fromfile) == 1 {
		if err := StreamToMap(fromfile[0], frompath, results, true); err != nil {
			return nil, err
		}
	}
	if len(tofile) == 1 {
		if err := StreamToMap(tofile[0], topath, results, false); err != nil {
			return nil, err
		}
	}
	if kv != nil {
		for _, val := range results {
			for k, v := range kv {
				val[k] = v
			}
		}
	}
	return results, nil
}

// Object is a schema model object interface
type Object interface {
	FromMap(kv map[string]interface{})
}

// CreateObject will create a new model object from kv
func CreateObject(object Object, kv map[string]interface{}, mapping map[string][]string) error {
	newkv := make(map[string]interface{})
	for k, vals := range mapping {
		for _, v := range vals {
			res, err := invokeAction(v, kv)
			if err != nil {
				return err
			}
			if res != nil {
				newkv[k] = res
				break
			}
			newkv[k] = res
		}
	}
	object.FromMap(newkv)
	return nil
}

// CloneMap makes a copy of map
func CloneMap(kv map[string]interface{}) map[string]interface{} {
	newkv := make(map[string]interface{})
	for k, v := range kv {
		newkv[k] = v
	}
	return newkv
}
