package httpmessage

import (
	"net/http"
	"strings"

	pjson "github.com/pinpt/go-common/json"
)

type templateParams struct {
	Title   string
	Message string
}

type jsonResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// RenderStatus will render an appropriate message detail
func RenderStatus(w http.ResponseWriter, req *http.Request, status int, title, message string) {
	w.WriteHeader(status)
	w.Header().Set("Cache-Control", "max-age=0,no-cache,no-store")
	accept := req.Header.Get("accept")
	ct := req.Header.Get("content-type")
	rw := req.Header.Get("x-requested-with") == "XMLHttpRequest"
	if strings.Contains(accept, "json") || rw || strings.Contains(ct, "json") {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(pjson.Stringify(jsonResponse{false, message})))
		return
	}
	w.Header().Set("Content-Type", "text/html")
	tmpl.Execute(w, templateParams{title, message})
}
