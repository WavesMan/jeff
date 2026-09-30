package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 大模型 API 通常有排队、限流和较长的生成时间，不能使用无超时的
// http.Post，也不能在收到 429/529 后立即重试，否则会进一步放大限流。
const (
	requestTimeout = 2 * time.Minute
	maxAttempts    = 4
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 8 * time.Second
)

var apiClient = &http.Client{
	Timeout: requestTimeout,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

// ==================== 大额题库 (50条不重复问题，避免缓存命中) ====================
var questionBank = []struct {
	State    string
	Question string
}{
	{State: "Refund request: the customer says the parcel arrived crushed and wants their money back.", Question: "Which team should handle this? Is the customer angry?"},
	{State: "Order delivery delayed by 3 days, customer threatens to leave negative review on social media.", Question: "Route to appropriate team and assess customer sentiment."},
	{State: "Product received is the wrong size - customer ordered XL but got M. Wants exchange.", Question: "Determine handling team and check if customer shows frustration."},
	{State: "Billing error: customer was charged twice for the same order totaling $298.", Question: "Identify correct department and evaluate anger level."},
	{State: "Customer wants to cancel subscription but system keeps showing error messages.", Question: "Route to tech or billing team and determine urgency."},
	{State: "Gift item arrived with visible damage to packaging, recipient is upset.", Question: "Assign to logistics team and check customer emotion state."},
	{State: "Loyalty points not credited after purchase of $150 worth of electronics.", Question: "Direct to rewards team and assess satisfaction level."},
	{State: "Customer reports missing items from delivered package - ordered 5 items got 3.", Question: "Route to fulfillment team and evaluate complaint severity."},
	{State: "Promo code SAVE20 not applying at checkout despite being active.", Question: "Send to payments team and check if customer is frustrated."},
	{State: "Repeat customer of 5 years says service quality has declined significantly.", Question: "Escalate to management team and measure sentiment."},
	{State: "International shipment stuck in customs for 2 weeks, customer anxious.", Question: "Route to international logistics and assess urgency."},
	{State: "Customer received expired product - skincare item past expiry date by 1 month.", Question: "Escalate to quality assurance and check customer anger."},
	{State: "Subscription auto-renewed despite cancellation request submitted last week.", Question: "Handle billing adjustment and determine customer sentiment."},
	{State: "Product description mismatch - advertised as organic but contains synthetic materials.", Question: "Route to product team and evaluate complaint level."},
	{State: "Customer wants to update shipping address but app crashes on save.", Question: "Assign to tech support and check frustration indicator."},
	{State: "Bulk order of 50 units for corporate event arrived 1 week late.", Question: "Route to enterprise sales and assess business impact."},
	{State: "Customer pet food order arrived opened with contents partially spilled.", Question: "Handle replacement and check customer emotion."},
	{State: "Price match request denied despite finding lower price at competitor.", Question: "Route to pricing team and evaluate customer satisfaction."},
	{State: "Gift card purchased but code never received via email after payment confirmation.", Question: "Send to payments team and check urgency level."},
	{State: "Customer with disability requests special packaging accommodation repeatedly ignored.", Question: "Escalate to customer success and assess severity."},
	{State: "Return label provided by company was wrong address - package sent to wrong facility.", Question: "Route to logistics and check customer frustration."},
	{State: "Flash sale item sold out within minutes, customer wants priority restock notification.", Question: "Handle via marketing team and measure interest level."},
	{State: "Customer child received toy with small parts detached - choking hazard concern.", Question: "Immediately escalate to safety team and assess urgency."},
	{State: "Wedding order of 200 custom cookies arrived with wrong designs printed.", Question: "Escalate to events team and evaluate business impact."},
	{State: "Medical supplies order delayed causing patient treatment schedule disruption.", Question: "Priority route to fulfillment and assess criticality."},
	{State: "Customer NFT digital purchase not reflected in wallet after blockchain confirmation.", Question: "Route to tech blockchain team and check urgency."},
	{State: "Restaurant reservation booking system double-booked same table for two parties.", Question: "Handle via operations and assess customer satisfaction."},
	{State: "Insurance claim documentation rejected due to technical file format issue.", Question: "Route to claims team and evaluate frustration level."},
	{State: "Freelancer platform payment held in escrow for 30 days past agreed terms.", Question: "Send to finance team and check contractor sentiment."},
	{State: "Smart home device firmware update bricked 5 devices during simultaneous update.", Question: "Escalate to product team and assess customer anger."},
	{State: "Grocery delivery arrived with 3 spoiled items and missing refrigeration packs.", Question: "Handle refund and check customer satisfaction."},
	{State: "Customer artwork commission received with completely wrong style ordered realistic got cartoon.", Question: "Route to artist relations and evaluate dissatisfaction."},
	{State: "Travel booking confirmed but airline check-in shows no reservation.", Question: "Emergency route to travel team and assess trip impact."},
	{State: "Software license key activated on wrong account belonging to different company.", Question: "Route to licensing team and check urgency."},
	{State: "Fitness tracker band broke after 2 weeks of normal use despite 1-year warranty.", Question: "Handle warranty claim and measure customer sentiment."},
	{State: "Live event ticket transferred to scalper platform customer wants refund.", Question: "Route to ticketing team and assess frustration."},
	{State: "Baby formula shipment contained counterfeit products matching external packaging.", Question: "Critical escalation to safety and legal teams immediately."},
	{State: "Home renovation contractor payment released but work quality severely substandard.", Question: "Route to dispute resolution and evaluate severity."},
	{State: "Cloud storage account hacked customer personal files leaked to public folder.", Question: "Critical security escalation and assess customer panic level."},
	{State: "Pet adoption fee paid but adoption paperwork never processed by shelter partner.", Question: "Route to partnerships team and check customer urgency."},
	{State: "Customer requested custom engraving on jewelry but received blank item without text.", Question: "Route to custom orders team and evaluate customer disappointment."},
	{State: "Subscription box contained wrong theme ordered nature theme got gaming theme instead.", Question: "Handle exchange and check customer satisfaction."},
	{State: "Delivery driver left package in rain without protective wrapping contents water damaged.", Question: "Route to logistics and assess customer anger level."},
	{State: "Customer loyalty tier downgrade caused previously approved discount to be revoked.", Question: "Route to retention team and evaluate urgency."},
	{State: "Pre-order item shipped from wrong warehouse resulting in 2-week additional delay.", Question: "Route to warehouse management and check customer patience level."},
	{State: "Customer payment info compromised after using saved card on platform.", Question: "Critical escalation to security team and assess customer fear level."},
	{State: "Bulk corporate order of branded merchandise arrived with wrong company logo printed.", Question: "Escalate to production team and evaluate business impact."},
	{State: "Customer elderly parent medication delivery missed scheduled window by 5 hours.", Question: "Priority escalation to health services and assess urgency."},
	{State: "Digital download license purchased but activation code shows as already used.", Question: "Route to digital services team and check customer frustration."},
	{State: "Customer customized furniture order arrived with wrong wood finish specified.", Question: "Route to manufacturing team and evaluate dissatisfaction."},
}

// ==================== 数据结构 ====================
type APIRequest struct {
	Model     string                 `json:"model"`
	State     string                 `json:"state"`
	Questions map[string]QuestionObj `json:"questions"`
}

type QuestionObj struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria,omitempty"`
}

type TestResult struct {
	ReqNum     int           `json:"req_num"`
	Latency    time.Duration `json:"latency"`
	StatusCode int           `json:"status_code"`
	Body       string        `json:"body"`
	Cache      string        `json:"cache_status"`
	Err        string        `json:"error,omitempty"`
}

const (
	cacheUnknown = "unknown"
	cacheHit     = "hit"
	cacheMiss    = "miss"

	modeRepeated = "repeated"
	modeUnique   = "unique"
	modeMixed    = "mixed"
)

type CacheStats struct {
	HitCount       int     `json:"hit_count"`
	MissCount      int     `json:"miss_count"`
	UnknownCount   int     `json:"unknown_count"`
	DetectedCount  int     `json:"detected_count"`
	HitRate        float64 `json:"hit_rate"`
	HitAvgLatency  string  `json:"hit_avg_latency"`
	MissAvgLatency string  `json:"miss_avg_latency"`
}

type TestReport struct {
	TotalRequests     int           `json:"total_requests"`
	SuccessCount      int           `json:"success_count"`
	FailureCount      int           `json:"failure_count"`
	SuccessRate       float64       `json:"success_rate"`
	AvgLatency        time.Duration `json:"avg_latency"`
	MinLatency        time.Duration `json:"min_latency"`
	MaxLatency        time.Duration `json:"max_latency"`
	P95Latency        time.Duration `json:"p95_latency"`
	P99Latency        time.Duration `json:"p99_latency"`
	RequestsPerSecond float64       `json:"requests_per_second"`
	ConcurrentLevel   int           `json:"concurrent_level"`
	TestDuration      time.Duration `json:"test_duration"`
	Resources         ResourceUsage `json:"resources"`
	Cache             CacheStats    `json:"cache"`
	RequestMode       string        `json:"request_mode"`
}

type ResourceUsage struct {
	CPUCores             int     `json:"cpu_cores"`
	AvgCPUPercent        float64 `json:"avg_cpu_percent"`
	PeakCPUPercent       float64 `json:"peak_cpu_percent"`
	AvgMemoryPercent     float64 `json:"avg_memory_percent"`
	PeakMemoryPercent    float64 `json:"peak_memory_percent"`
	GPUPresent           bool    `json:"gpu_present"`
	AvgGPUPercent        float64 `json:"avg_gpu_percent"`
	PeakGPUPercent       float64 `json:"peak_gpu_percent"`
	AvgGPUMemoryPercent  float64 `json:"avg_gpu_memory_percent"`
	PeakGPUMemoryPercent float64 `json:"peak_gpu_memory_percent"`
	PeakGPUTemperatureC  float64 `json:"peak_gpu_temperature_c"`
}

type resourceSample struct {
	cpuPercent     float64
	memoryPercent  float64
	gpuPercent     float64
	gpuMemory      float64
	gpuTemperature float64
	gpuPresent     bool
}

type resourceMonitor struct {
	mu      sync.Mutex
	samples []resourceSample
	stop    chan struct{}
	done    chan struct{}
}

func readCPUStat() (idle, total uint64, ok bool) {
	data, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:]
		var values []uint64
		for _, field := range fields {
			value, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return 0, 0, false
			}
			values = append(values, value)
		}
		if len(values) < 4 {
			return 0, 0, false
		}
		for _, value := range values {
			total += value
		}
		idle = values[3]
		if len(values) > 4 {
			idle += values[4] // iowait
		}
		return idle, total, true
	}
	return 0, 0, false
}

func readMemoryPercent() (float64, bool) {
	data, err := ioutil.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	values := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				values[strings.TrimSuffix(fields[0], ":")] = value
			}
		}
	}
	total, available := values["MemTotal"], values["MemAvailable"]
	if total == 0 || available > total {
		return 0, false
	}
	return float64(total-available) / float64(total) * 100, true
}

func readGPU() (utilization, memoryPercent, temperature float64, present bool) {
	cmd := exec.Command("nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu", "--format=csv,noheader,nounits")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, 0, false
	}
	fields := strings.Split(strings.TrimSpace(strings.Split(string(output), "\n")[0]), ",")
	if len(fields) < 4 {
		return 0, 0, 0, false
	}
	parse := func(value string) float64 {
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return parsed
	}
	utilization = parse(fields[0])
	used, total := parse(fields[1]), parse(fields[2])
	if total > 0 {
		memoryPercent = used / total * 100
	}
	temperature = parse(fields[3])
	return utilization, memoryPercent, temperature, true
}

func sampleResources(previousIdle, previousTotal *uint64) resourceSample {
	sample := resourceSample{}
	if idle, total, ok := readCPUStat(); ok {
		if *previousTotal > 0 && total > *previousTotal {
			deltaTotal := total - *previousTotal
			deltaIdle := idle - *previousIdle
			sample.cpuPercent = (1 - float64(deltaIdle)/float64(deltaTotal)) * 100
		}
		*previousIdle, *previousTotal = idle, total
	}
	sample.memoryPercent, _ = readMemoryPercent()
	sample.gpuPercent, sample.gpuMemory, sample.gpuTemperature, sample.gpuPresent = readGPU()
	return sample
}

func newResourceMonitor() *resourceMonitor {
	monitor := &resourceMonitor{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(monitor.done)
		var previousIdle, previousTotal uint64
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sample := sampleResources(&previousIdle, &previousTotal)
				monitor.mu.Lock()
				monitor.samples = append(monitor.samples, sample)
				monitor.mu.Unlock()
			case <-monitor.stop:
				return
			}
		}
	}()
	return monitor
}

func (monitor *resourceMonitor) finish() ResourceUsage {
	close(monitor.stop)
	<-monitor.done
	monitor.mu.Lock()
	samples := append([]resourceSample(nil), monitor.samples...)
	monitor.mu.Unlock()
	usage := ResourceUsage{CPUCores: runtime.NumCPU()}
	if len(samples) == 0 {
		return usage
	}
	for _, sample := range samples {
		usage.AvgCPUPercent += sample.cpuPercent
		usage.AvgMemoryPercent += sample.memoryPercent
		usage.AvgGPUPercent += sample.gpuPercent
		usage.AvgGPUMemoryPercent += sample.gpuMemory
		if sample.cpuPercent > usage.PeakCPUPercent {
			usage.PeakCPUPercent = sample.cpuPercent
		}
		if sample.memoryPercent > usage.PeakMemoryPercent {
			usage.PeakMemoryPercent = sample.memoryPercent
		}
		if sample.gpuPercent > usage.PeakGPUPercent {
			usage.PeakGPUPercent = sample.gpuPercent
		}
		if sample.gpuMemory > usage.PeakGPUMemoryPercent {
			usage.PeakGPUMemoryPercent = sample.gpuMemory
		}
		if sample.gpuTemperature > usage.PeakGPUTemperatureC {
			usage.PeakGPUTemperatureC = sample.gpuTemperature
		}
		usage.GPUPresent = usage.GPUPresent || sample.gpuPresent
	}
	count := float64(len(samples))
	usage.AvgCPUPercent /= count
	usage.AvgMemoryPercent /= count
	usage.AvgGPUPercent /= count
	usage.AvgGPUMemoryPercent /= count
	return usage
}

// HTTP 请求没有传输错误，不代表接口调用成功。
// 只有 2xx 响应才算成功；例如 529 必须计入失败。
func isHTTPSuccess(statusCode int) bool {
	return statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices
}

func shouldRetry(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests ||
		statusCode == 529 ||
		statusCode == http.StatusInternalServerError ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}

func retryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
}

func backoff(attempt int, serverWait time.Duration) time.Duration {
	delay := initialBackoff
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= maxBackoff {
			delay = maxBackoff
			break
		}
	}
	if serverWait > delay {
		delay = serverWait
	}
	// 加少量 jitter，避免大量请求在同一时刻再次冲击 API。
	jitter := time.Duration(rand.Int63n(int64(delay/5 + 1)))
	return delay + jitter
}

// ==================== 格式化输出 ====================
func printHeader(title string) {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("  %s\n", title)
	fmt.Println(strings.Repeat("=", 70))
}

func printSection(title string) {
	fmt.Println("\n" + strings.Repeat("-", 50))
	fmt.Printf("  >> %s\n", title)
	fmt.Println(strings.Repeat("-", 50))
}

func printKV(key, value string) {
	fmt.Printf("    %-22s : %s\n", key, value)
}

func printProgressBar(current, total int, width int) {
	percent := float64(current) / float64(total)
	filled := int(percent * float64(width))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	fmt.Printf("\r    [%s] %d/%d (%.1f%%)", bar, current, total, percent*100)
	if current == total {
		fmt.Println()
	}
}

func normalizeCacheValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "hit", "true", "1", "cached":
		return cacheHit
	case "miss", "false", "0", "uncached":
		return cacheMiss
	default:
		return cacheUnknown
	}
}

// 只有服务端明确返回缓存元数据时才判定命中/未命中，避免用延迟猜测缓存状态。
func detectCacheStatus(resp *http.Response, body string) string {
	if resp != nil {
		for _, name := range []string{"X-Cache-Hit", "X-Cache", "X-Cache-Status"} {
			if status := normalizeCacheValue(resp.Header.Get(name)); status != cacheUnknown {
				return status
			}
		}
	}
	var payload map[string]interface{}
	if json.Unmarshal([]byte(body), &payload) == nil {
		if value, ok := payload["cache_hit"].(bool); ok {
			if value {
				return cacheHit
			}
			return cacheMiss
		}
		if value, ok := payload["cache_status"].(string); ok {
			if status := normalizeCacheValue(value); status != cacheUnknown {
				return status
			}
		}
		if value, ok := payload["model_called"].(bool); ok {
			if value {
				return cacheMiss
			}
			return cacheHit
		}
	}
	return cacheUnknown
}

func requestState(q struct {
	State    string
	Question string
}, mode string, reqNum int, runID int64) string {
	if mode == modeUnique || (mode == modeMixed && reqNum%5 == 0) {
		return fmt.Sprintf("%s\n[load-test-unique-id:%d-%d]", q.State, runID, reqNum)
	}
	return q.State
}

// ==================== 单次请求 ====================
func makeRequest(targetURL string, qIdx int, mode string, runID int64) TestResult {
	q := questionBank[qIdx%len(questionBank)]

	req := APIRequest{
		Model: "jeff-latest",
		State: requestState(q, mode, qIdx, runID),
		Questions: map[string]QuestionObj{
			"route": {
				Type:         "choice",
				Instructions: q.Question,
				Criteria: map[string]string{
					"1": "Refunds and payments",
					"2": "Damaged or lost parcels",
					"3": "Account and login problems",
				},
			},
			"angry": {
				Type:         "noul",
				Instructions: "Is the customer angry?",
			},
		},
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return TestResult{ReqNum: qIdx + 1, Err: fmt.Sprintf("marshal error: %v", err)}
	}

	reqStart := time.Now()
	var lastErr error
	var lastStatus int
	var lastBody string
	lastCache := cacheUnknown
	attempts := 0

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(reqBody))
		if err != nil {
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := apiClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts {
				time.Sleep(backoff(attempt, 0))
				continue
			}
			break
		}

		lastStatus = resp.StatusCode
		respBody, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		lastBody = string(respBody)
		lastCache = detectCacheStatus(resp, lastBody)

		if isHTTPSuccess(lastStatus) || !shouldRetry(lastStatus) || attempt == maxAttempts {
			break
		}
		time.Sleep(backoff(attempt, retryAfter(resp)))
	}

	result := TestResult{
		ReqNum:     qIdx + 1,
		Latency:    time.Since(reqStart),
		StatusCode: lastStatus,
		Body:       lastBody,
		Cache:      lastCache,
	}
	if lastErr != nil {
		result.Err = lastErr.Error()
	} else if !isHTTPSuccess(lastStatus) {
		result.Err = fmt.Sprintf("HTTP status %d after %d attempts", lastStatus, attempts)
	}
	return result
}

// ==================== 并发测试 ====================
func runConcurrentTest(targetURL string, concurrency int, totalRequests int, mode string) ([]TestResult, TestReport) {
	printSection(fmt.Sprintf("并发测试 | 模式: %s | 并发数: %d | 总请求: %d", mode, concurrency, totalRequests))

	var results []TestResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	startTime := time.Now()
	monitor := newResourceMonitor()
	runID := startTime.UnixNano()
	var successCount int64
	var failureCount int64
	var reqCounter int64

	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(reqNum int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// qIdx 使用实际请求序号；函数内部再按题库长度轮询。
			// 这样 unique 模式能保证每个逻辑请求的标识都不同。
			result := makeRequest(targetURL, reqNum, mode, runID)

			mu.Lock()
			results = append(results, result)
			if result.Err == "" {
				atomic.AddInt64(&successCount, 1)
			} else {
				atomic.AddInt64(&failureCount, 1)
			}
			mu.Unlock()

			counter := atomic.AddInt64(&reqCounter, 1)
			printProgressBar(int(counter), totalRequests, 40)
		}(i)
	}

	wg.Wait()
	testDuration := time.Since(startTime)
	resources := monitor.finish()

	// 按请求号排序
	for i := 1; i < len(results); i++ {
		key := results[i]
		j := i - 1
		for j >= 0 && results[j].ReqNum > key.ReqNum {
			results[j+1] = results[j]
			j--
		}
		results[j+1] = key
	}

	// 计算统计
	var totalLatency time.Duration
	var minLatency time.Duration = time.Hour
	var maxLatency time.Duration = 0
	var latencies []time.Duration

	for _, r := range results {
		totalLatency += r.Latency
		latencies = append(latencies, r.Latency)
		if r.Latency < minLatency {
			minLatency = r.Latency
		}
		if r.Latency > maxLatency {
			maxLatency = r.Latency
		}
	}

	n := len(results)
	avgLatency := time.Duration(int64(totalLatency) / int64(n))

	// P95/P99 排序
	for i := 1; i < len(latencies); i++ {
		key := latencies[i]
		j := i - 1
		for j >= 0 && latencies[j] > key {
			latencies[j+1] = latencies[j]
			j--
		}
		latencies[j+1] = key
	}

	p95Idx := int(math.Floor(float64(n) * 0.95))
	p99Idx := int(math.Floor(float64(n) * 0.99))
	if p95Idx >= n {
		p95Idx = n - 1
	}
	if p99Idx >= n {
		p99Idx = n - 1
	}

	succ := atomic.LoadInt64(&successCount)
	fail := atomic.LoadInt64(&failureCount)
	successRate := float64(succ) / float64(n) * 100
	rps := float64(n) / testDuration.Seconds()
	cache := CacheStats{}
	var hitLatency, missLatency time.Duration
	for _, result := range results {
		switch result.Cache {
		case cacheHit:
			cache.HitCount++
			hitLatency += result.Latency
		case cacheMiss:
			cache.MissCount++
			missLatency += result.Latency
		default:
			cache.UnknownCount++
		}
	}
	cache.DetectedCount = cache.HitCount + cache.MissCount
	if cache.DetectedCount > 0 {
		cache.HitRate = float64(cache.HitCount) / float64(cache.DetectedCount) * 100
	}
	if cache.HitCount > 0 {
		cache.HitAvgLatency = (hitLatency / time.Duration(cache.HitCount)).String()
	}
	if cache.MissCount > 0 {
		cache.MissAvgLatency = (missLatency / time.Duration(cache.MissCount)).String()
	}

	report := TestReport{
		TotalRequests:     n,
		SuccessCount:      int(succ),
		FailureCount:      int(fail),
		SuccessRate:       successRate,
		AvgLatency:        avgLatency,
		MinLatency:        minLatency,
		MaxLatency:        maxLatency,
		P95Latency:        latencies[p95Idx],
		P99Latency:        latencies[p99Idx],
		RequestsPerSecond: rps,
		ConcurrentLevel:   concurrency,
		TestDuration:      testDuration,
		Resources:         resources,
		Cache:             cache,
		RequestMode:       mode,
	}

	return results, report
}

// ==================== 打印报告 ====================
func printReport(report TestReport) {
	printSection("测试报告")

	fmt.Println()
	printKV("请求模式", report.RequestMode)
	printKV("并发 / 请求数", fmt.Sprintf("%d / %d", report.ConcurrentLevel, report.TotalRequests))
	printKV("重试后成功 / 失败", fmt.Sprintf("%d / %d", report.SuccessCount, report.FailureCount))
	printKV("重试后成功率", fmt.Sprintf("%.2f%%", report.SuccessRate))
	fmt.Println()
	printKV("测试耗时", report.TestDuration.String())
	printKV("平均延迟", report.AvgLatency.String())
	printKV("P95 延迟", report.P95Latency.String())
	printKV("P99 延迟", report.P99Latency.String())
	printKV("最小 / 最大延迟", fmt.Sprintf("%s / %s", report.MinLatency, report.MaxLatency))
	printKV("逻辑请求吞吐", fmt.Sprintf("%.2f 请求/秒", report.RequestsPerSecond))
	fmt.Println()
	printKV("缓存 HIT / MISS / 未知", fmt.Sprintf("%d / %d / %d", report.Cache.HitCount, report.Cache.MissCount, report.Cache.UnknownCount))
	if report.Cache.DetectedCount > 0 {
		printKV("已识别请求命中率", fmt.Sprintf("%.2f%%（分母 %d，不含未知）", report.Cache.HitRate, report.Cache.DetectedCount))
		if report.Cache.HitCount > 0 {
			printKV("HIT 平均延迟", report.Cache.HitAvgLatency)
		}
		if report.Cache.MissCount > 0 {
			printKV("MISS 平均延迟", report.Cache.MissAvgLatency)
		}
	} else {
		printKV("缓存命中率", "未知（服务端未返回缓存标识）")
	}
	fmt.Println()
	printKV("CPU 平均/峰值", fmt.Sprintf("%.1f%% / %.1f%% (%d 核)", report.Resources.AvgCPUPercent, report.Resources.PeakCPUPercent, report.Resources.CPUCores))
	printKV("RAM 平均/峰值", fmt.Sprintf("%.1f%% / %.1f%%", report.Resources.AvgMemoryPercent, report.Resources.PeakMemoryPercent))
	if report.Resources.GPUPresent {
		printKV("GPU 利用率 平均/峰值", fmt.Sprintf("%.1f%% / %.1f%%（整机采样）", report.Resources.AvgGPUPercent, report.Resources.PeakGPUPercent))
		printKV("显存占用 平均/峰值", fmt.Sprintf("%.1f%% / %.1f%%", report.Resources.AvgGPUMemoryPercent, report.Resources.PeakGPUMemoryPercent))
		printKV("GPU 峰值温度", fmt.Sprintf("%.1f°C", report.Resources.PeakGPUTemperatureC))
	} else {
		printKV("GPU 监控", "N/A（未检测到 nvidia-smi/GPU）")
	}
	fmt.Println()
}

// ==================== 寻找最大并发量 ====================
func findMaxConcurrency(targetURL string, mode string) TestReport {
	printHeader("并发压力测试 - 寻找最大稳定并发量")

	concurrencyLevels := []int{5, 10, 20, 50, 100, 200, 500}
	// 每一档至少发起与并发级别相同数量的请求，否则“并发 500、总请求 100”
	// 实际上最多只有 100 个请求同时运行，无法验证 500 并发。
	requestsPerLevel := 100

	var bestReport TestReport
	var lastReport TestReport
	var bestConcurrency int
	var reports []TestReport

	for _, conc := range concurrencyLevels {
		requests := requestsPerLevel
		if requests < conc {
			requests = conc
		}
		_, report := runConcurrentTest(targetURL, conc, requests, mode)
		reports = append(reports, report)
		lastReport = report

		// 大模型 API 允许少量瞬时限流/服务端失败：成功率 >= 85% 且平均延迟 < 5s。
		isStable := report.SuccessRate >= 85.0 && report.AvgLatency < 5*time.Second
		if isStable {
			bestReport = report
			bestConcurrency = conc
			fmt.Printf("\n  [INFO] 并发 %d 达到阈值，继续测试下一档。\n", conc)
		} else {
			fmt.Printf("\n  [WARN] 并发 %d 未达阈值 (重试后成功率=%.1f%%, 平均延迟=%v)，停止递增。\n",
				conc, report.SuccessRate, report.AvgLatency)
			break
		}
	}

	printSection("并发档位汇总")
	fmt.Printf("  %-10s %-10s %-18s %-13s %-12s %-10s %-10s\n", "并发", "请求数", "最终成功/失败", "成功率", "命中率*", "P95", "请求/秒")
	fmt.Println("  " + strings.Repeat("-", 91))
	for _, report := range reports {
		hitRate := "未知"
		if report.Cache.DetectedCount > 0 {
			hitRate = fmt.Sprintf("%.1f%%", report.Cache.HitRate)
		}
		fmt.Printf("  %-10d %-10d %-18s %-13s %-12s %-10s %-10.2f\n",
			report.ConcurrentLevel, report.TotalRequests,
			fmt.Sprintf("%d/%d", report.SuccessCount, report.FailureCount),
			fmt.Sprintf("%.1f%%", report.SuccessRate), hitRate,
			report.P95Latency.Round(time.Millisecond), report.RequestsPerSecond)
	}
	fmt.Println("  * 命中率仅用服务端明确返回 HIT/MISS 的请求计算，未知状态不纳入分母。")

	if bestConcurrency == 0 {
		bestReport = lastReport
		fmt.Printf("\n  [WARN] 首档并发未达到稳定阈值，以实际测试结果（并发 %d）为准\n", bestReport.ConcurrentLevel)
	} else {
		fmt.Printf("\n  [RESULT] 最大稳定并发量: %d\n", bestConcurrency)
	}

	return bestReport
}

// HTTP 响应全部收齐后，再向服务端确认没有仍在执行的 systemone 请求。
// 这避免客户端已经退出，而服务端终端日志/模型任务仍在收尾。
func waitForServerIdle(targetURL string, timeout time.Duration) bool {
	healthURL := strings.TrimSuffix(targetURL, "/v1/systemone") + "/health"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := apiClient.Get(healthURL)
		if err == nil {
			body, readErr := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil {
				var health struct {
					Status         string `json:"status"`
					ActiveRequests int    `json:"active_requests"`
				}
				if json.Unmarshal(body, &health) == nil && health.Status == "ready" && health.ActiveRequests == 0 {
					return true
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// ==================== 生成报告文件 ====================
func generateReportFile(report TestReport, outputPath string) {
	jsonReport := fmt.Sprintf(`{
  "test_info": {
    "target_url": "http://127.0.0.1:8766/v1/systemone",
    "question_bank_size": %d
  },
  "results": {
    "request_mode": "%s",
    "concurrent_level": %d,
    "total_requests": %d,
    "success_count": %d,
    "failure_count": %d,
    "success_rate": %.2f,
    "test_duration": "%s",
    "avg_latency": "%s",
    "min_latency": "%s",
    "max_latency": "%s",
    "p95_latency": "%s",
    "p99_latency": "%s",
    "requests_per_second": %.2f,
    "cache": {
      "hit_count": %d,
      "miss_count": %d,
      "unknown_count": %d,
      "detected_count": %d,
      "hit_rate": %.2f,
      "hit_avg_latency": "%s",
      "miss_avg_latency": "%s"
    },
    "resources": {
      "cpu_cores": %d,
      "avg_cpu_percent": %.2f,
      "peak_cpu_percent": %.2f,
      "avg_memory_percent": %.2f,
      "peak_memory_percent": %.2f,
      "gpu_present": %t,
      "avg_gpu_percent": %.2f,
      "peak_gpu_percent": %.2f,
      "avg_gpu_memory_percent": %.2f,
      "peak_gpu_memory_percent": %.2f,
      "peak_gpu_temperature_c": %.2f
    }
  }
}`, len(questionBank),
		report.RequestMode,
		report.ConcurrentLevel,
		report.TotalRequests,
		report.SuccessCount,
		report.FailureCount,
		report.SuccessRate,
		report.TestDuration.String(),
		report.AvgLatency.String(),
		report.MinLatency.String(),
		report.MaxLatency.String(),
		report.P95Latency.String(),
		report.P99Latency.String(),
		report.RequestsPerSecond,
		report.Cache.HitCount,
		report.Cache.MissCount,
		report.Cache.UnknownCount,
		report.Cache.DetectedCount,
		report.Cache.HitRate,
		report.Cache.HitAvgLatency,
		report.Cache.MissAvgLatency,
		report.Resources.CPUCores,
		report.Resources.AvgCPUPercent,
		report.Resources.PeakCPUPercent,
		report.Resources.AvgMemoryPercent,
		report.Resources.PeakMemoryPercent,
		report.Resources.GPUPresent,
		report.Resources.AvgGPUPercent,
		report.Resources.PeakGPUPercent,
		report.Resources.AvgGPUMemoryPercent,
		report.Resources.PeakGPUMemoryPercent,
		report.Resources.PeakGPUTemperatureC,
	)

	ioutil.WriteFile(outputPath, []byte(jsonReport), 0644)
	fmt.Printf("\n  [INFO] JSON 报告已保存: %s\n", outputPath)
}

// ==================== 主函数 ====================
func main() {
	targetURL := "http://127.0.0.1:8766/v1/systemone"
	mode := flag.String("mode", modeMixed, "请求模式: repeated、unique 或 mixed")
	flag.Parse()
	if args := flag.Args(); len(args) > 0 {
		*mode = args[0]
	}
	if *mode != modeRepeated && *mode != modeUnique && *mode != modeMixed {
		fmt.Printf("无效的请求模式 %q，可选值: %s、%s、%s\n", *mode, modeRepeated, modeUnique, modeMixed)
		return
	}

	printHeader("SystemOne API 并发压力测试工具")

	fmt.Println()
	printKV("目标地址", targetURL)
	printKV("题库大小", fmt.Sprintf("%d 条不重复问题", len(questionBank)))
	printKV("每并发级别请求数", "至少等于并发级别（最低 100）")
	printKV("请求模式", *mode)
	printKV("模式参数", "repeated | unique | mixed（也支持 -mode 参数）")
	if *mode == modeMixed {
		printKV("混合模式比例", "80% 重复 + 20% 唯一")
	}

	// 健康检查
	fmt.Println("\n  >> 健康检查")
	healthStart := time.Now()
	resp, err := http.Get(targetURL)
	healthLatency := time.Since(healthStart)

	if err != nil {
		simpleReq, _ := json.Marshal(map[string]string{"model": "test", "state": "health check"})
		resp, err = http.Post(targetURL, "application/json", bytes.NewBuffer(simpleReq))
	}

	if err != nil {
		fmt.Printf("    [ERROR] 无法连接到服务器: %v\n", err)
		fmt.Println("    [INFO] 服务器不可达，测试将无法执行。请确保服务已启动。")
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Printf("    [OK] 服务器可达 | 延迟: %v | 响应: %s\n", healthLatency.Round(time.Millisecond), string(body))
		resp.Body.Close()
	}

	// 运行并发测试
	fmt.Println()
	finalReport := findMaxConcurrency(targetURL, *mode)
	if waitForServerIdle(targetURL, 30*time.Second) {
		fmt.Println("\n  [INFO] 服务端已确认空闲，测试完整结束")
	} else {
		fmt.Println("\n  [WARN] 在 30 秒内未收到服务端 active_requests=0 的确认，报告仍将保存")
	}

	// 输出最终报告
	printHeader("最终测试报告")
	printReport(finalReport)

	// 生成报告文件
	generateReportFile(finalReport, "/home/gem/workhub/output/api_concurrency_test_report.json")
}
