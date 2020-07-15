package os

import (
	"bytes"
	"io/ioutil"
	"runtime"

	"github.com/pinpt/go-common/v10/fileutil"
)

const k8sServiceAcct = "/var/run/secrets/kubernetes.io/serviceaccount"

// IsInsideContainer returns true if the process is running inside a containerized environment like docker or kubernetes
func IsInsideContainer() bool {
	if runtime.GOOS == "linux" {
		if fileutil.FileExists("/proc/self/cgroup") {
			buf, _ := ioutil.ReadFile("/proc/self/cgroup")
			// check either native docker or docker inside k8s
			if bytes.Contains(buf, []byte("docker")) || bytes.Contains(buf, []byte("kubepods")) {
				return true
			}
		}
		if fileutil.FileExists(k8sServiceAcct) {
			return true
		}
	}
	return false
}
