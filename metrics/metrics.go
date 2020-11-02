package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RequestDurationMilliseconds is the metric
	RequestDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pinpoint_request_duration_milliseconds",
		Help:    "latency of a given service - operation",
		Buckets: []float64{5, 10, 25, 50, 100, 500, 1000, 5000},
	}, []string{"service", "operation"})

	// RequestsTotal is the metric
	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pinpoint_requests_total",
		Help: "total number of requests by service - operation",
	}, []string{"service", "operation", "response_code"})

	// RequestBytesSum is the metric
	RequestBytesSum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pinpoint_request_bytes_sum",
		Help: "sum of bytes for a request by service - operation",
	}, []string{"service", "operation"})

	// ResponseBytesSum is the metric
	ResponseBytesSum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pinpoint_response_bytes_sum",
		Help: "sum of bytes for a response by service - operation",
	}, []string{"service", "operation"})

)

func init() {
	prometheus.MustRegister(RequestDurationMilliseconds)
	prometheus.MustRegister(RequestsTotal)
	prometheus.MustRegister(RequestBytesSum)
	prometheus.MustRegister(ResponseBytesSum)
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
}
