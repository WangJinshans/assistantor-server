package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// variables for monitoring
	upstreamBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "upstream_bytes",
			Help:      "upstream bytes",
		},
	)
	downstreamBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "downstream_bytes",
			Help:      "downstream bytes",
		},
	)
	packageSent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "sent_packages",
			Help:      "sent packges",
		},
	)

	packageReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "received_packages",
			Help:      "received packages",
		},
	)

	verifySuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "verify_success_packages",
			Help:      "verify success packages",
		},
	)

	errorPackages = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "error_packages",
			Help:      "",
		},
	)

	enqueuedPackages = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "kafka",
			Name:      "enqueued_packages",
			Help:      "",
			Buckets:   []float64{1, 10, 20, 30, 40, 50, 100, 1000},
		},
	)
	producedPackages = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "kafka",
			Name:      "produced_packages",
			Help:      "",
			Buckets:   []float64{1, 10, 20, 30, 40, 50, 100, 1000},
		},
	)

	connectionCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gateway",
			Subsystem: "connection",
			Name:      "connection_count",
			Help:      "connection count",
		},
	)

	apiCallsSendCommand = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "api_calls",
			Name:      "send_command",
			Help:      "Number of API calls",
		},
	)
	apiCallsQuery = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "api_calls",
			Name:      "query",
			Help:      "Number of API calls",
		},
	)
	apiCallsHandleResponse = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "api_calls",
			Name:      "handle_response",
			Help:      "Number of API calls",
		},
	)
	packageBacklogged = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "package",
			Name:      "backlogged",
			Help:      "",
		},
	)
	packageSendSucceeded = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "package",
			Name:      "send_succeeded",
			Help:      "",
		},
	)
	packageSendFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "package",
			Name:      "send_failed",
			Help:      "",
		},
	)
	packageResponseSucceeded = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "package",
			Name:      "response_succeeded",
			Help:      "",
		},
	)
	packageResponseFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "wrapped_command_sender",
			Subsystem: "package",
			Name:      "response_failed",
			Help:      "",
		},
	)
)

func prometheusRegiste() {
	// traffic
	prometheus.Register(upstreamBytes)
	prometheus.Register(downstreamBytes)
	prometheus.Register(packageSent)

	prometheus.Register(packageReceived)
	prometheus.Register(verifySuccess)

	// kafka
	prometheus.Register(enqueuedPackages)
	prometheus.Register(producedPackages)
	prometheus.Register(errorPackages)

	// conn
	prometheus.Register(connectionCount)
}

func prometheusRegisteWrappedCommandSender() {
	prometheus.Register(apiCallsSendCommand)
	prometheus.Register(apiCallsQuery)
	prometheus.Register(apiCallsHandleResponse)

	// packages
	prometheus.Register(packageBacklogged)
	prometheus.Register(packageSendSucceeded)
	prometheus.Register(packageSendFailed)
	prometheus.Register(packageResponseSucceeded)
	prometheus.Register(packageResponseFailed)
}
