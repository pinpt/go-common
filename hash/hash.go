package hash

import (
	"github.com/jhaynie/go-gator/orm"
)

// Values converts passed objects to strings and returns a hash of the concatenated values.
// Uses xxhash which is not cryptographically secure.
// Is OK to use non cryptographically secure hash, since we use hashing mainly for generating consistent ids, key values or equality checks.
func Values(objects ...interface{}) string {
	return orm.HashValues(objects...)
}
