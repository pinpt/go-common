//go:generate pigeon -optimize-parser -o grammar.go grammar.peg

package filterexpr

import (
	"strings"
)

// Filter is a filter which can be evaluated to true or false based on the input map
type Filter interface {
	// Test will return true if the filter expression matches the map
	Test(kv map[string]interface{}) bool
}

type filter struct {
	expr *ExpressionGroup
}

var _ Filter = (*filter)(nil)

// Test will return true if the filter expression matches the map
func (f *filter) Test(kv map[string]interface{}) bool {
	return f.expr.Test(kv)
}

// Compile will compile the filter expression as a string in Filter which can be saved and invoked and is thread safe
func Compile(expr string) (Filter, error) {
	object, err := ParseReader("", strings.NewReader(expr), MaxExpressions(5000))
	if err != nil {
		return nil, err
	}
	// fmt.Println(object)
	return &filter{object.(*ExpressionGroup)}, nil
}
