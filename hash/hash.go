package hash

import (
	"fmt"
	"io"

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
		case int, int32, int64:
			fmt.Fprintf(h, "%d", s)
		case float32, float64:
			fmt.Fprintf(h, "%f", s)
		case *string:
			if s != nil {
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
				fmt.Fprintf(h, "%f", *s)
			}
		case *float64:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%f", *s)
			}
		case *bool:
			if s == nil {
				io.WriteString(h, "")
			} else {
				fmt.Fprintf(h, "%v", *s)
			}
		default:
			fmt.Fprintf(h, "%v", s)
		}
	}

	return fmt.Sprintf("%016x", h.Sum64())
}
