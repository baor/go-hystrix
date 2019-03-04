package main

import (
	"sync"
	"time"

	p8s "github.com/prometheus/client_golang/prometheus"
)

var buckets = []float64{
	0.05,
	0.1,
	0.3,
	0.5,
	0.7,
	1,
	1.5,
	2,
	3,
	5,
	8,
	12,
	17,
	25,
	50}

var labels = []string{
	"group",
	"command"}

var threadsCompleted = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_threads_completed"})
var threadsActive = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_threads_active"})
var commandConcurrentExecution = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_concurrentexecution"})
var commandSuccess = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_success"})
var commandFailure = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_failure"})
var commandTimeout = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_timeout"})
var commandShortCircuited = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_shortcorcuited"})
var commandFallbackSuccess = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_fallbacksuccess"})
var commandFallbackRejection = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_fallbackrejection"})
var commandFallbackMissing = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_fallbackmissing"})
var commandExceptionThrown = p8s.NewGauge(p8s.GaugeOpts{Name: "app_hystrix_command_exceptionthrown"})
var commandExecutionTimeSeconds = p8s.NewHistogram(p8s.HistogramOpts{Name: "app_hystrix_command_executiontime_seconds", Buckets: buckets})
var threadExecutionTimeSeconds = p8s.NewHistogram(p8s.HistogramOpts{Name: "app_hystrix_thread_executiontime_seconds", Buckets: buckets})

type hystrixCircuitBreaker struct {
	commands *hystrixCommands
	metrics  *hystrixMetrics
}

type hystrixCommand struct {
	circuitOpenMutex              sync.RWMutex
	healthMutex                   sync.RWMutex
	isCircuitOpen                 bool
	circuitOpenedOrLastTestedTime time.Time
	options                       *hystrixCommandOptions
	metricLabels                  string
	lastHealthSnapshot            time.Time
	healthSnapshot                *HystrixHealthSnapshot
	metrics                       *CommandMetrics
}
type hystrixCommands map[string]*hystrixCommand

// hystrixCommandOptions is used to tune circuit settings at runtime
type hystrixCommandOptions struct {
	CommandTimeoutInMilliseconds                  time.Duration `json:"CommandTimeoutInMilliseconds"`
	CircuitBreakerForcedOpen                      bool          `json:"CircuitBreakerForcedOpen"`
	CircuitBreakerForcedClosed                    bool          `json:"CircuitBreakerForcedClosed"`
	CircuitBreakerErrorThresholdPercentage        int           `json:"CircuitBreakerErrorThresholdPercentage"`
	CircuitBreakerSleepWindowInMilliseconds       time.Duration `json:"CircuitBreakerSleepWindowInMilliseconds"`
	CircuitBreakerRequestVolumeThreshold          int           `json:"CircuitBreakerRequestVolumeThreshold"`
	MetricsHealthSnapshotIntervalInMilliseconds   time.Duration `json:"MetricsHealthSnapshotIntervalInMilliseconds"`
	MetricsRollingStatisticalWindowInMilliseconds time.Duration `json:"MetricsRollingStatisticalWindowInMilliseconds"`
	MetricsRollingStatisticalWindowBuckets        int           `json:"MetricsRollingStatisticalWindowBuckets"`
	MetricsRollingPercentileEnabled               bool          `json:"MetricsRollingPercentileEnabled"`
	MetricsRollingPercentileWindowInMilliseconds  time.Duration `json:"MetricsRollingPercentileWindowInMilliseconds"`
	MetricsRollingPercentileWindowBuckets         int           `json:"MetricsRollingPercentileWindowBuckets"`
	MetricsRollingPercentileBucketSize            int           `json:"MetricsRollingPercentileBucketSize"`
	HystrixCommandEnabled                         bool          `json:"HystrixCommandEnabled"`
}

func NewHystrixCommandOptions() *hystrixCommandOptions {
	return &hystrixCommandOptions{
		CommandTimeoutInMilliseconds:                  1000,
		CircuitBreakerForcedOpen:                      false,
		CircuitBreakerForcedClosed:                    false,
		CircuitBreakerErrorThresholdPercentage:        50,
		CircuitBreakerSleepWindowInMilliseconds:       5000,
		CircuitBreakerRequestVolumeThreshold:          20,
		MetricsHealthSnapshotIntervalInMilliseconds:   500,
		MetricsRollingStatisticalWindowInMilliseconds: 10000,
		HystrixCommandEnabled:                         true}
}

var defaultBuckets = []float64{
	0.05,
	0.1,
	0.3,
	0.5,
	0.7,
	1,
	1.5,
	2,
	3,
	5,
	8,
	12,
	17,
	25,
	50}

type hystrixMetrics struct {
	ThreadsCompleted           p8s.Gauge
	ThreadsActive              p8s.Gauge
	CommandConcurrentExecution p8s.Gauge
	CommandSuccess             p8s.Gauge
	CommandFailure             p8s.Gauge
	CommandTimeout             p8s.Gauge
	CommandShortCircuited      p8s.Gauge
	CommandFallbackSuccess     p8s.Gauge
	CommandFallbackRejection   p8s.Gauge
	CommandFallbackMissing     p8s.Gauge
	CommandExceptionThrown     p8s.Gauge
	CommandExecutionTime       p8s.Histogram
	ThreadExecutionTime        p8s.Histogram
}

type CommandMetrics struct {
	success, failure, timeout int
}

type HystrixHealthSnapshot struct {
	totalCount, errorCount int
	errorPercentage        int
}

func (hc *hystrixCommand) getHealthCounts() *HystrixHealthSnapshot {
	currentTime := time.Now()
	sinceLastTime := currentTime.Sub(hc.lastHealthSnapshot)

	if sinceLastTime < hc.options.MetricsHealthSnapshotIntervalInMilliseconds {
		return hc.healthSnapshot
	}

	success := hc.metrics.success
	failure := hc.metrics.failure
	timeout := hc.metrics.timeout
	totalCount := failure + success + timeout
	errorCount := failure + timeout
	errorPercentage := 0

	if totalCount > 0 {
		errorPercentage = (int)(errorCount / totalCount * 100)
	}

	hc.circuitOpenMutex.RLock()
	hc.lastHealthSnapshot = currentTime
	hc.healthSnapshot = &HystrixHealthSnapshot{totalCount: totalCount, errorPercentage: errorPercentage}
	hc.circuitOpenMutex.Unlock()
	return hc.healthSnapshot
}

func (hc *hystrixCommand) isOpen() bool {
	if hc.isCircuitOpen {
		return true
	}

	// we're closed, so let's see if errors have made us so we should trip the circuit open
	healthCounts := hc.getHealthCounts()

	// check if we are past the CircuitBreakerRequestVolumeThreshold
	if healthCounts.totalCount < hc.options.CircuitBreakerRequestVolumeThreshold {
		// we are not past the minimum volume threshold for the statisticalWindow so we'll return false immediately and not calculate anything
		return false
	}

	// if error percentage is below threshold the circuit remains closed
	if healthCounts.errorPercentage < hc.options.CircuitBreakerErrorThresholdPercentage {
		return false
	}

	// failure rate is too high, trip the circuit (multiple threads can come to these lines, but do we care?)
	hc.openCircuit()

	return true
}

func (hc *hystrixCommand) openCircuit() {
	if hc.isCircuitOpen {
		return
	}

	//log.WarnFormat("Circuit breaker for group {0} and key {1} has opened.", commandIdentifier.GroupKey, commandIdentifier.CommandKey);

	hc.circuitOpenMutex.Lock()
	hc.isCircuitOpen = true
	hc.circuitOpenedOrLastTestedTime = time.Now()
	hc.circuitOpenMutex.Unlock()
}

func (hc *hystrixCommand) allowSingleTest() bool {

	if hc.isCircuitOpen && (time.Since(hc.circuitOpenedOrLastTestedTime) > hc.options.CircuitBreakerSleepWindowInMilliseconds) {
		//log.InfoFormat("Allowing single test request through circuit breaker for group {0} and key {1}.", commandIdentifier.GroupKey, commandIdentifier.CommandKey)

		// this thread is the first one here and can do a canary request
		hc.circuitOpenMutex.RLock()
		hc.circuitOpenedOrLastTestedTime = time.Now()
		hc.circuitOpenMutex.RUnlock()
		return true
	}

	return false

}

func (hc *hystrixCommand) allowRequest() bool {
	if hc.options.CircuitBreakerForcedOpen {
		return false
	}

	if hc.options.CircuitBreakerForcedClosed {
		return true
	}
	return !hc.isCircuitOpen || hc.allowSingleTest()
}

func (hc *hystrixCommand) resetCounter() {
	hc.healthMutex.Lock()
	hc.metrics.success = 0
	hc.metrics.failure = 0
	hc.metrics.timeout = 0

	hc.lastHealthSnapshot = time.Now()
	hc.healthSnapshot = &HystrixHealthSnapshot{errorCount: 0, errorPercentage: 0, totalCount: 0}
	hc.healthMutex.Unlock()
}

func main() {

}
