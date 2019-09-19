package term

import pos "github.com/pinpt/go-common/os"

var fixedTermWidth uint

func init() {
	w := pos.GetenvInt("TERMWIDTH", 0)
	if w > 0 {
		fixedTermWidth = uint(w)
	}
}
