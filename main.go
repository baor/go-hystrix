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

var labelNames = []string{"group", "command"}
var threadsCompleted = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_threads_completed"}, labelNames)
var threadsActive = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_threads_active"}, labelNames)
var commandConcurrentExecution = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_concurrentexecution"}, labelNames)
var commandSuccess = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_success"}, labelNames)
var commandFailure = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_failure"}, labelNames)
var commandTimeout = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_timeout"}, labelNames)
var commandShortCircuited = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_shortcorcuited"}, labelNames)
var commandFallbackSuccess = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_fallbacksuccess"}, labelNames)
var commandFallbackRejection = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_fallbackrejection"}, labelNames)
var commandFallbackMissing = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_fallbackmissing"}, labelNames)
var commandExceptionThrown = p8s.NewGaugeVec(p8s.GaugeOpts{Name: "app_hystrix_command_exceptionthrown"}, labelNames)
var commandExecutionTimeSeconds = p8s.NewHistogramVec(p8s.HistogramOpts{Name: "app_hystrix_command_executiontime_seconds", Buckets: buckets}, labelNames)
var threadExecutionTimeSeconds = p8s.NewHistogramVec(p8s.HistogramOpts{Name: "app_hystrix_thread_executiontime_seconds", Buckets: buckets}, labelNames)

func init() {
	p8s.MustRegister(threadsCompleted)
	p8s.MustRegister(threadsActive)
	p8s.MustRegister(commandConcurrentExecution)
}

type hystrixCircuitBreaker struct {
	commands *hystrixCommands
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
	labels                        []string
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

func (hc *hystrixCommand) MarkSuccess() {
	commandSuccess.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkFailure() {
	commandFailure.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkTimeout() {
	commandTimeout.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkShortCircuited() {
	commandShortCircuited.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) IncrementConcurrentExecutionCount() {
	commandConcurrentExecution.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) DecrementConcurrentExecutionCount() {
	commandConcurrentExecution.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkFallbackSuccess() {
	commandFallbackSuccess.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkFallbackRejection() {
	commandFallbackRejection.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkFallbackMissing() {
	commandFallbackMissing.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkExceptionThrown() {
	commandExceptionThrown.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) AddCommandExecutionTime(seconds float64) {
	commandExecutionTimeSeconds.WithLabelValues(hc.labels...).Observe(seconds)
}

func (hc *hystrixCommand) AddUserThreadExecutionTime(seconds float64) {
	threadExecutionTimeSeconds.WithLabelValues(hc.labels...).Observe(seconds)
}

func (hc *hystrixCommand) MarkThreadExecution() {
	threadsActive.WithLabelValues(hc.labels...).Inc()
}

func (hc *hystrixCommand) MarkThreadCompletion() {
	threadsActive.WithLabelValues(hc.labels...).Dec()
	threadsCompleted.WithLabelValues(hc.labels...).Inc()
}

func main() {

}
