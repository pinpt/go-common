package metrics

import (
	"fmt"
	"context"
	"net/http"

	"github.com/pinpt/go-common/v10/log"
	pos "github.com/pinpt/go-common/v10/os"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartServer starts the metricserver
func StartServer(ctx context.Context, logger log.Logger) {
	// run webserver
	http.Handle("/metrics", promhttp.Handler())

	// expose a `/ready` endpoint so we can standardize readiness checks & liveness checks
	http.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// clients will be expected to implement custom /health checks based on a service's requirements

	server := &http.Server{
		Addr: ":81", // always start on port 81 so we don't clobber any thing that's running on another port
	}
	go func() {
		log.Debug(logger, "starting /metrics endpoint port 81")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			select {
			case <-ctx.Done():
			default:
				log.Fatal(logger, "error starting Metrics server", "err", err)
			}
		}
	}()

	pprofPort := pos.Getenv("PP_PROFILE_PORT", "")
	if len(pprofPort) > 0 {
		log.Debug(logger, "pprof listening on port ", "pprofPort", pprofPort)

		go func() {
			fmt.Println(http.ListenAndServe(":"+pprofPort, nil))
		}()
	}
}
