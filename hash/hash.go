package hash

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/cespare/xxhash"
)

// Values will convert all objects to a string and return a SHA256 of the concatenated values.
// Uses xxhash to calculate a faster hash value that is not cryptographically secure but is OK since
// we use hashing mainfully for generating consistent key values or equality checks.
func Values(objects ...interface{}) string {
	return hashValues(objects...)
}

func hashValues(objects ...interface{}) string {
	h := xxhash.New()

	//This is a type switch, pointers have to be handled differently unfortunately
	//Note: It appears fmt.Fprintf is second fastest to io.WriteString.
	for _, o := range objects {
		switch s := o.(type) {
		case string:
			io.WriteString(h, s)
		case []byte:
			h.Write(s)
		case []string:
			for _, v := range s {
				io.WriteString(h, v)
			}
		case bool:
			if s {
				io.WriteString(h, "true")
			} else {
				io.WriteString(h, "false")
			}
		case int, int8, int16, int32, int64:
			fmt.Fprintf(h, "%d", s)
		case float32:
			// truncate without decimals if a float like 123.00
			if s == float32(int32(s)) {
				fmt.Fprintf(h, "%d", int32(s))
			} else {
				fmt.Fprintf(h, "%f", s)
			}
		case float64:
			// truncate without decimals if a float like 123.00
			if s == float64(int64(s)) {
				fmt.Fprintf(h, "%d", int64(s))
			} else {
				fmt.Fprintf(h, "%f", s)
			}
		case *string:
			if s == nil {
				io.WriteString(h, "")
			} else {
				io.WriteString(h, *s)
			}
		case *int:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%d", *s)
			}
		case *int8:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%d", *s)
			}
		case *int16:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%d", *s)
			}
		case *int32:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%d", *s)
			}
		case *int64:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%d", *s)
			}
		case *float32:
			if s == nil {
				io.WriteString(h, "")
			} else {
				// truncate without decimals if a float like 123.00
				if *s == float32(int32(*s)) {
					fmt.Fprintf(h, "%d", int32(*s))
				} else {
					fmt.Fprintf(h, "%f", *s)
				}
			}
		case *float64:
			if s == nil {
				io.WriteString(h, "")
			} else {
				// truncate without decimals if a float like 123.00
				if *s == float64(int64(*s)) {
					fmt.Fprintf(h, "%d", int64(*s))
				} else {
					fmt.Fprintf(h, "%f", *s)
				}
			}
		case *bool:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%v", *s)
			}
		default:
			if s == nil {
				io.WriteString(h, "")
			} else {
				t := reflect.TypeOf(s)
				k := t.Kind()
				if k == reflect.Ptr {
					s = reflect.ValueOf(s).Interface()
					k = reflect.Interface
				}
				switch k {
				case reflect.Struct, reflect.Slice, reflect.Interface:
					buf, _ := json.Marshal(s)
					h.Write(buf)
				default:
					// fmt.Println(reflect.TypeOf(s), reflect.TypeOf(s).Kind(), fmt.Sprintf("%v", s))
					fmt.Fprintf(h, "%v", s)
				}
			}
		}
	}

	return fmt.Sprintf("%016x", h.Sum64())
}
