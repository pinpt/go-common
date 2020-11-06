package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RequestDurationMilliseconds is the metric
	RequestDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pinpoint_request_duration_milliseconds",
		Help:    "latency of a given service - operation",
		Buckets: []float64{5, 10, 25, 50, 75, 100, 200, 500, 800, 1000, 3000, 5000},
	}, []string{"service", "operation", "response_code"})

	// RequestsTotal is the metric
	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pinpoint_requests_total",
		Help: "total number of requests by service - operation",
	}, []string{"service", "operation", "response_code"})

	// WebsocketConnections is the metric
	WebsocketConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pinpoint_websocket_connections",
		Help: "number of websocket connections for a request by service",
	}, []string{"service"})

	// RequestBytes is the metric
	RequestBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pinpoint_request_bytes",
		Help:    "size of request bytes in a given service - operation",
		Buckets: []float64{.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 300000, 600000, 1800000, 3600000},
	}, []string{"service", "operation", "response_code"})

	// ResponseBytes is the metric
	ResponseBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pinpoint_response_bytes",
		Help:    "size of response bytes in a given service - operation",
		Buckets: []float64{.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 300000, 600000, 1800000, 3600000},
	}, []string{"service", "operation", "response_code"})
)

func init() {
	prometheus.MustRegister(RequestDurationMilliseconds)
	prometheus.MustRegister(RequestsTotal)
	prometheus.MustRegister(RequestBytes)
	prometheus.MustRegister(ResponseBytes)
	prometheus.MustRegister(WebsocketConnections)
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
}
