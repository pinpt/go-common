package strings

import (
	"fmt"
	"regexp"
	"strings"
)

var re = regexp.MustCompile(`(\$?{(.*?)})`)

// InterpolateString will replace { } in string
func InterpolateString(val string, env map[string]interface{}) (string, error) {
	if val == "" {
		return val, nil
	}
	errors := make(chan error, 100)
	val = re.ReplaceAllStringFunc(val, func(s string) string {
		tok := re.FindStringSubmatch(s)
		key := tok[2]
		def := s
		var required bool
		if key[0:1] == "!" {
			key = key[1:]
			required = true
		}
		idx := strings.Index(key, ":-")
		if idx > 0 {
			def = key[idx+2:]
			key = key[0:idx]
		}
		v := env[key]
		if v == nil {
			if required {
				errors <- fmt.Errorf("required value not found for key '%s'", key)
			}
			return def
		}
		switch v.(type) {
		case string:
			if v == "" {
				if required {
					errors <- fmt.Errorf("required value not found for key '%s'", key)
				}
				return def
			}
		}
		return fmt.Sprintf("%v", v)
	})
	select {
	case err := <-errors:
		return "", err
	default:
	}
	return val, nil
}
