package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	greenstack "github.com/matheusjuliosantana/green-stack-monitor"
)

var (
	est *greenstack.Estimator
)

func init() {
	var err error
	est, err = greenstack.NewEstimator(greenstack.DefaultBrazilConfig())
	if err != nil {
		log.Fatalf("greenstack: failed to create estimator: %v", err)
	}
}

var (
	startTime   = time.Now()
	totalReqs   int64
	cacheHits   int64
	cacheMisses int64
	hitsToday   int64

	todayStr = time.Now().Format("2006-01-02")
	todayMu  sync.Mutex
)

func checkDayReset() {
	todayMu.Lock()
	defer todayMu.Unlock()
	if d := time.Now().Format("2006-01-02"); d != todayStr {
		todayStr = d
		atomic.StoreInt64(&hitsToday, 0)
	}
}

type responseWindow struct {
	mu   sync.Mutex
	vals []float64
}

func (w *responseWindow) record(ms float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.vals = append(w.vals, ms)
	if len(w.vals) > 100 {
		w.vals = w.vals[1:]
	}
}

func (w *responseWindow) p50() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.vals) == 0 {
		return 0
	}
	tmp := make([]float64, len(w.vals))
	copy(tmp, w.vals)
	sort.Float64s(tmp)
	return tmp[len(tmp)/2]
}

var rtWindow = &responseWindow{}

const (
	bucketCapacity  = 30.0
	fillRatePerMs   = 0.002
	cleanupInterval = 5 * time.Minute
	bucketIdleTTL   = 10 * time.Minute
)

type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
	lastSeen time.Time
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := float64(now.Sub(b.lastFill).Milliseconds())
	b.tokens = min(bucketCapacity, b.tokens+elapsed*fillRatePerMs)
	b.lastFill = now
	b.lastSeen = now
	if b.tokens >= 1.0 {
		b.tokens--
		return true
	}
	return false
}

type rateLimiter struct {
	mu      sync.RWMutex
	buckets map[string]*tokenBucket
}

func newRateLimiter() *rateLimiter {
	rl := &rateLimiter{buckets: make(map[string]*tokenBucket)}
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			rl.cleanup()
		}
	}()
	return rl
}

func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.RLock()
	b, ok := rl.buckets[ip]
	rl.mu.RUnlock()
	if !ok {
		rl.mu.Lock()
		if b, ok = rl.buckets[ip]; !ok {
			b = &tokenBucket{tokens: bucketCapacity, lastFill: time.Now(), lastSeen: time.Now()}
			rl.buckets[ip] = b
		}
		rl.mu.Unlock()
	}
	return b.allow()
}

func (rl *rateLimiter) cleanup() {
	cutoff := time.Now().Add(-bucketIdleTTL)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for ip, b := range rl.buckets {
		b.mu.Lock()
		idle := b.lastSeen.Before(cutoff)
		b.mu.Unlock()
		if idle {
			delete(rl.buckets, ip)
		}
	}
}

var limiter = newRateLimiter()

func realIP(r *http.Request) string {
	for _, h := range []string{"Fly-Client-IP", "CF-Connecting-IP"} {
		if ip := parseFirstIP(r.Header.Get(h)); ip != "" {
			return ip
		}
	}
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && ip != "" {
		return ip
	}
	return r.RemoteAddr
}

func parseFirstIP(value string) string {
	if value == "" {
		return ""
	}
	ip := strings.TrimSpace(strings.SplitN(value, ",", 2)[0])
	if net.ParseIP(ip) == nil {
		return ""
	}
	return ip
}

const maxCacheEntries = 200

type cacheEntry struct {
	body        []byte
	contentType string
	expiresAt   time.Time
}

type memCacheStore struct {
	mu sync.RWMutex
	m  map[string]cacheEntry
}

func newMemCache() *memCacheStore {
	c := &memCacheStore{m: make(map[string]cacheEntry)}
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			c.evict()
		}
	}()
	return c
}

func (c *memCacheStore) get(key string) (cacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[key]
	if !ok || time.Now().After(e.expiresAt) {
		return cacheEntry{}, false
	}
	return e, true
}

func (c *memCacheStore) set(key string, body []byte, ct string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= maxCacheEntries {
		now := time.Now()
		for k, v := range c.m {
			if now.After(v.expiresAt) {
				delete(c.m, k)
			}
		}
	}
	c.m[key] = cacheEntry{body: body, contentType: ct, expiresAt: time.Now().Add(ttl)}
}

func (c *memCacheStore) evict() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.m {
		if now.After(v.expiresAt) {
			delete(c.m, k)
		}
	}
}

var cache = newMemCache()

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

type bodyRecorder struct {
	http.ResponseWriter
	code int
	body []byte
}

func (r *bodyRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *bodyRecorder) Write(b []byte) (int, error) {
	r.body = append(r.body, b...)
	return r.ResponseWriter.Write(b)
}

type gzipWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipWriter) Write(b []byte) (int, error) { return g.gz.Write(b) }
func (g *gzipWriter) WriteHeader(code int) {
	g.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("PANIC: %v\n%s", err, debug.Stack())
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withSecurity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
		h.Set("Content-Security-Policy",
			"default-src 'self'; "+
				"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "+
				"font-src https://fonts.gstatic.com; "+
				"script-src 'self' 'unsafe-inline'; "+
				"connect-src 'self'; "+
				"img-src 'self' data:; "+
				"frame-ancestors 'none'")
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}

func withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := realIP(r)
		if !limiter.allow(ip) {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, `{"error":"rate limit exceeded","retry_after":1}`)
			log.Printf("RATE_LIMIT ip=%s path=%s", ip, r.URL.Path)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(rec, r)
		log.Printf("%s %s %d %dms ip=%s",
			r.Method, r.URL.Path, rec.code,
			time.Since(start).Milliseconds(), realIP(r))
	})
}

func withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		defer gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		next.ServeHTTP(&gzipWriter{ResponseWriter: w, gz: gz}, r)
	})
}

func withCache(ttl time.Duration, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		atomic.AddInt64(&totalReqs, 1)
		checkDayReset()

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if ttl > 0 {
			if e, ok := cache.get(cacheKey(r)); ok {
				atomic.AddInt64(&cacheHits, 1)
				atomic.AddInt64(&hitsToday, 1)
				w.Header().Set("Content-Type", e.contentType)
				w.Header().Set("X-Cache", "HIT")
				w.Write(e.body)
				dur := time.Since(start)
				rtWindow.record(float64(dur.Milliseconds()))
				est.Record(dur, true)
				return
			}
			atomic.AddInt64(&cacheMisses, 1)
		}

		rec := &bodyRecorder{ResponseWriter: w, code: 200}
		h(rec, r)
		dur := time.Since(start)
		rtWindow.record(float64(dur.Milliseconds()))
		est.Record(dur, false)

		if ttl > 0 && rec.code == 200 && len(rec.body) > 0 {
			cache.set(cacheKey(r), rec.body, rec.Header().Get("Content-Type"), ttl)
		}
	}
}

func cacheKey(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return r.URL.Path
	}
	return r.URL.Path + "?" + r.URL.RawQuery
}

func chain(ttl time.Duration, h http.HandlerFunc) http.Handler {
	return withRecovery(withSecurity(withRateLimit(withLogger(withGzip(
		http.HandlerFunc(withCache(ttl, h)),
	)))))
}

func chainNocache(h http.HandlerFunc) http.Handler {
	return withRecovery(withSecurity(withRateLimit(withLogger(withGzip(
		http.HandlerFunc(h),
	)))))
}

type carbonReading struct {
	IntensityGCO2KWh float64
	Zone             string
	UpdatedAt        time.Time
}

var carbonStore = struct {
	sync.RWMutex
	reading *carbonReading
}{}

func fetchCarbonIntensity() (float64, string) {
	apiKey := os.Getenv("ELECTRICITY_MAPS_KEY")
	if apiKey == "" {
		return 0, ""
	}
	carbonStore.RLock()
	if r := carbonStore.reading; r != nil && time.Since(r.UpdatedAt) < time.Hour {
		v, z := r.IntensityGCO2KWh, r.Zone
		carbonStore.RUnlock()
		return v, z
	}
	carbonStore.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.electricitymap.org/v3/carbon-intensity/latest?zone=BR-CS", nil)
	if err != nil {
		return 0, ""
	}
	req.Header.Set("auth-token", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("carbon API error: %v", err)
		return 0, ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("carbon API status: %d", resp.StatusCode)
		return 0, ""
	}

	var result struct {
		CarbonIntensity float64 `json:"carbonIntensity"`
		Zone            string  `json:"zone"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		log.Printf("carbon API decode error: %v", err)
		return 0, ""
	}

	carbonStore.Lock()
	carbonStore.reading = &carbonReading{
		IntensityGCO2KWh: result.CarbonIntensity,
		Zone:             result.Zone,
		UpdatedAt:        time.Now(),
	}
	carbonStore.Unlock()
	log.Printf("carbon intensity: %.1f gCO₂/kWh zone=%s", result.CarbonIntensity, result.Zone)
	return result.CarbonIntensity, result.Zone
}

type MetricsResponse struct {
	CacheHitRate           float64 `json:"cache_hit_rate"`
	P50ResponseMs          float64 `json:"p50_response_ms"`
	CachedRequestsToday    int64   `json:"cached_requests_today"`
	UptimeHours            float64 `json:"uptime_hours"`
	CO2PerReqG             float64 `json:"co2_per_req_g"`
	CO2TotalG              float64 `json:"co2_total_g"`
	CO2SavedG              float64 `json:"co2_saved_g"`
	BadgeColor             string  `json:"badge_color"`
	CarbonIntensityGCO2KWh float64 `json:"carbon_intensity_gco2kwh"`
	CarbonZone             string  `json:"carbon_zone,omitempty"`
	TotalRequests          int64   `json:"total_requests"`
	CacheHits              int64   `json:"cache_hits"`
	CacheMisses            int64   `json:"cache_misses"`
	GeneratedAt            string  `json:"generated_at"`
}

type ecoSummary struct {
	co2PerReqG  float64
	co2TotalG   float64
	co2SavedG   float64
	hitRate     float64
	hits        int64
	misses      int64
	uptimeHours float64
}

func buildEcoSummary() ecoSummary {
	hits := atomic.LoadInt64(&cacheHits)
	misses := atomic.LoadInt64(&cacheMisses)
	total := hits + misses
	hr := 0.0
	if total > 0 {
		hr = float64(hits) / float64(total) * 100
	}
	snap := est.Snapshot()
	return ecoSummary{
		co2PerReqG:  snap.PerReqG,
		co2TotalG:   snap.TotalG,
		co2SavedG:   snap.SavedG,
		hitRate:     hr,
		hits:        hits,
		misses:      misses,
		uptimeHours: time.Since(startTime).Hours(),
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	s := buildEcoSummary()
	carbonIntensity, carbonZone := fetchCarbonIntensity()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(MetricsResponse{
		CacheHitRate:           s.hitRate,
		P50ResponseMs:          rtWindow.p50(),
		CachedRequestsToday:    atomic.LoadInt64(&hitsToday),
		UptimeHours:            s.uptimeHours,
		CO2PerReqG:             s.co2PerReqG,
		CO2TotalG:              s.co2TotalG,
		CO2SavedG:              s.co2SavedG,
		BadgeColor:             greenstack.BadgeColor(s.co2PerReqG),
		CarbonIntensityGCO2KWh: carbonIntensity,
		CarbonZone:             carbonZone,
		TotalRequests:          atomic.LoadInt64(&totalReqs),
		CacheHits:              s.hits,
		CacheMisses:            s.misses,
		GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
	})
}

func ecoHandler(w http.ResponseWriter, r *http.Request) {
	s := buildEcoSummary()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"co2_per_request_g":   fmt.Sprintf("%.6f", s.co2PerReqG),
		"co2_total_session_g": fmt.Sprintf("%.4f", s.co2TotalG),
		"co2_saved_session_g": fmt.Sprintf("%.4f", s.co2SavedG),
		"badge_color":         greenstack.BadgeColor(s.co2PerReqG),
		"cache_hit_rate":      fmt.Sprintf("%.1f%%", s.hitRate),
		"cache_hits_total":    s.hits,
		"uptime_hours":        fmt.Sprintf("%.1f", s.uptimeHours),
		"formula":             "CO₂(g) = duration_ms × TDP × PUE × CI / 3_600_000",
		"methodology":         "Green Algorithms — doi.org/10.1002/advs.202100707",
		"config":              "TDP=4W PUE=1.2 CI=100gCO₂/kWh (BR-CS grid)",
		"powered_by":          "github.com/matheusjuliosantana/green-stack-monitor",
		"note":                "wall time, not CPU time — conservative estimate",
	})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	html, err := os.ReadFile("portfolio.html")
	if err != nil {
		log.Fatalf("portfolio.html not found: %v", err)
	}
	log.Printf("portfolio.html: %dKB in memory", len(html)/1024)

	mux := http.NewServeMux()

	ogImageHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		http.ServeFile(w, r, "og-image.png")
	}
	mux.Handle("/og-image.png", chain(24*time.Hour, ogImageHandler))
	mux.Handle("/og-image-f09785a.png", chain(24*time.Hour, ogImageHandler))

	mux.Handle("/", chain(5*time.Minute, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=300")
		if _, err := io.Copy(w, strings.NewReader(string(html))); err != nil {
			log.Printf("failed to serve HTML: %v", err)
		}
	}))

	mux.Handle("/api/metrics", chainNocache(metricsHandler))
	mux.Handle("/eco", chain(30*time.Second, ecoHandler))
	mux.Handle("/api/ping", chainNocache(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","uptime_hours":%.1f,"ts":"%s"}`,
			time.Since(startTime).Hours(),
			time.Now().UTC().Format(time.RFC3339),
		)
	}))

	srv := &http.Server{
		Addr:           ":" + port,
		Handler:        mux,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 14,
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("forced shutdown: %v", err)
		}
	}()

	log.Printf("eco-portfolio on :%s", port)
	log.Printf("   /            -> portfolio  (5 min cache, gzip)")
	log.Printf("   /api/metrics -> metrics    (green-stack-monitor)")
	log.Printf("   /eco         -> eco ping   (30s cache)")
	log.Printf("   /api/ping    -> health")
	log.Printf("   rate limit   -> 30 burst, 2/sec per IP")
	log.Printf("   CO2 config   -> TDP=4W PUE=1.2 CI=100gCO2/kWh (BR-CS)")

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
