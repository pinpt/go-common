package os

import (
	"bytes"
	"io/ioutil"

	"github.com/pinpt/go-common/fileutil"
)

// IsInsideContainer returns true if the process is running inside a containerized environment like docker or kubernetes
func IsInsideContainer() bool {
	if fileutil.FileExists("/proc/self/cgroup") {
		buf, _ := ioutil.ReadFile("/proc/self/cgroup")
		// check either native docker or docker inside k8s
		if bytes.Contains(buf, []byte("docker")) || bytes.Contains(buf, []byte("kubepods")) {
			return true
		}
	}
	return false
}
