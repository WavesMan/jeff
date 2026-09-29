package main

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	listen, jeffURL, apiKey           string
	maxQueue, batchSize, cacheEntries int
	batchWait, timeout                time.Duration
}

func value(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if n, e := strconv.Atoi(os.Getenv(k)); e == nil && n > 0 {
		return n
	}
	return d
}
func configFromEnv() config {
	return config{value("GATEWAY_LISTEN", ":8080"), value("JEFF_URL", "http://jeff:8765"), os.Getenv("GATEWAY_API_KEY"), envInt("MAX_QUEUE", 32), envInt("BATCH_SIZE", 4), envInt("CACHE_ENTRIES", 256), time.Duration(envInt("BATCH_WAIT_MS", 3)) * time.Millisecond, time.Duration(envInt("REQUEST_TIMEOUT_MS", 2000)) * time.Millisecond}
}

type judgeRequest struct {
	Model     string                     `json:"model"`
	State     json.RawMessage            `json:"state"`
	Questions map[string]json.RawMessage `json:"questions"`
	Images    []string                   `json:"images,omitempty"`
}

func (r judgeRequest) valid() bool { return r.Model != "" && len(r.State) > 0 && len(r.Questions) > 0 }

type outcome struct {
	body json.RawMessage
	err  error
}
type pending struct {
	request judgeRequest
	key     string
	result  chan outcome
}
type entry struct {
	key   string
	value json.RawMessage
}
type lru struct {
	mu    sync.Mutex
	limit int
	items map[string]*list.Element
	order *list.List
}

func newLRU(n int) *lru { return &lru{limit: n, items: map[string]*list.Element{}, order: list.New()} }
func (c *lru) get(k string) (json.RawMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(e)
	return append(json.RawMessage(nil), e.Value.(entry).value...), true
}
func (c *lru) put(k string, v json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		e.Value = entry{k, append(json.RawMessage(nil), v...)}
		c.order.MoveToFront(e)
		return
	}
	e := c.order.PushFront(entry{k, append(json.RawMessage(nil), v...)})
	c.items[k] = e
	if c.order.Len() > c.limit {
		old := c.order.Back()
		delete(c.items, old.Value.(entry).key)
		c.order.Remove(old)
	}
}

type metrics struct{ requests, cacheHits, queueFull, upstreamErrors atomic.Uint64 }
type batcher struct {
	cfg     config
	client  *http.Client
	queue   chan pending
	cache   *lru
	metrics *metrics
}

func newBatcher(c config, m *metrics) *batcher {
	b := &batcher{c, &http.Client{}, make(chan pending, c.maxQueue), newLRU(c.cacheEntries), m}
	go b.run()
	return b
}
func fingerprint(r judgeRequest) string {
	raw, _ := json.Marshal(r)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

var errQueueFull = errors.New("gateway queue is full")

func (b *batcher) submit(ctx context.Context, r judgeRequest) (json.RawMessage, bool, error) {
	key := fingerprint(r)
	if v, ok := b.cache.get(key); ok {
		b.metrics.cacheHits.Add(1)
		return v, true, nil
	}
	p := pending{r, key, make(chan outcome, 1)}
	select {
	case b.queue <- p:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
		b.metrics.queueFull.Add(1)
		return nil, false, errQueueFull
	}
	select {
	case x := <-p.result:
		return x.body, false, x.err
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}
func (b *batcher) run() {
	for first := range b.queue {
		group := []pending{first}
		timer := time.NewTimer(b.cfg.batchWait)
	collect:
		for len(group) < b.cfg.batchSize {
			select {
			case p := <-b.queue:
				group = append(group, p)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.dispatch(group)
	}
}
func (b *batcher) dispatch(group []pending) {
	requests := make([]judgeRequest, len(group))
	for i, p := range group {
		requests[i] = p.request
	}
	payload, _ := json.Marshal(map[string]any{"model": "jeff-latest", "requests": requests})
	var response struct {
		Answers []json.RawMessage `json:"answers"`
	}
	err := b.call(payload, &response)
	if err != nil || len(response.Answers) != len(group) {
		if err == nil {
			err = errors.New("Jeff returned an incomplete batch")
		}
		b.metrics.upstreamErrors.Add(1)
		for _, p := range group {
			p.result <- outcome{err: err}
		}
		return
	}
	for i, p := range group {
		body, _ := json.Marshal(map[string]any{"result": json.RawMessage(response.Answers[i]), "cached": false, "policy": policy(response.Answers[i])})
		b.cache.put(p.key, body)
		p.result <- outcome{body: body}
	}
}
func (b *batcher) call(payload []byte, out any) error {
	var last error
	for _, delay := range []time.Duration{0, 20 * time.Millisecond, 80 * time.Millisecond} {
		if delay > 0 {
			time.Sleep(delay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), b.cfg.timeout)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.jeffURL+"/v1/systemone:batch", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if k := os.Getenv("JEFF_API_KEY"); k != "" {
			req.Header.Set("Authorization", "Bearer "+k)
		}
		res, err := b.client.Do(req)
		cancel()
		if err == nil {
			if res.StatusCode >= 200 && res.StatusCode < 300 {
				defer res.Body.Close()
				return json.NewDecoder(res.Body).Decode(out)
			}
			res.Body.Close()
			if res.StatusCode != 529 && res.StatusCode != 503 {
				return fmt.Errorf("Jeff returned HTTP %d", res.StatusCode)
			}
			err = fmt.Errorf("Jeff returned HTTP %d", res.StatusCode)
		}
		last = err
	}
	return last
}
func policy(a json.RawMessage) string {
	var response struct {
		Answers map[string]struct {
			Confidence *float64 `json:"confidence"`
		} `json:"answers"`
	}
	if json.Unmarshal(a, &response) != nil || len(response.Answers) == 0 {
		return "conservative"
	}
	result := "auto"
	for _, answer := range response.Answers {
		if answer.Confidence == nil || *answer.Confidence < .70 {
			return "conservative"
		}
		if *answer.Confidence < .90 {
			result = "review"
		}
	}
	return result
}
func main() {
	cfg := configFromEnv()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	m := &metrics{}
	b := newBatcher(cfg, m)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		res, e := b.client.Get(cfg.jeffURL + "/health")
		if e != nil || res.StatusCode != 200 {
			if res != nil {
				res.Body.Close()
			}
			http.Error(w, "Jeff is not ready", 503)
			return
		}
		res.Body.Close()
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "jeff_gateway_requests_total %d\njeff_gateway_cache_hits_total %d\njeff_gateway_queue_full_total %d\njeff_gateway_upstream_errors_total %d\njeff_gateway_queue_depth %d\n", m.requests.Load(), m.cacheHits.Load(), m.queueFull.Load(), m.upstreamErrors.Load(), len(b.queue))
	})
	mux.HandleFunc("POST /v1/judge", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, cfg.apiKey) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", 401)
			return
		}
		var body judgeRequest
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
		d.DisallowUnknownFields()
		if d.Decode(&body) != nil || !body.valid() {
			http.Error(w, "invalid judge request", 422)
			return
		}
		m.requests.Add(1)
		ctx, cancel := context.WithTimeout(r.Context(), cfg.timeout)
		defer cancel()
		answer, cached, e := b.submit(ctx, body)
		if e != nil {
			status := 502
			if errors.Is(e, errQueueFull) {
				status = 429
			}
			if errors.Is(e, context.DeadlineExceeded) {
				status = 504
			}
			http.Error(w, e.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if cached {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
		w.Write(answer)
	})
	srv := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("gateway listening", "address", cfg.listen)
	if e := srv.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
		log.Error("server failed", "error", e)
		os.Exit(1)
	}
}
func authorized(r *http.Request, key string) bool {
	if key == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+key)) == 1
}
