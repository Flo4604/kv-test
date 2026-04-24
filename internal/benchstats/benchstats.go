// Package benchstats is a tiny latency/throughput summary helper used
// across the spike harnesses. It avoids external deps so harnesses
// boot fast and stay easy to read.
package benchstats

import (
	"sort"
	"sync"
	"time"
)

// Recorder collects per-op latencies and the total wall time of the run.
// Safe for concurrent use.
type Recorder struct {
	mu      sync.Mutex
	samples []time.Duration
	errors  int
}

// NewRecorder returns a recorder with a pre-allocated sample slice.
func NewRecorder(hint int) *Recorder {
	if hint < 0 {
		hint = 0
	}
	return &Recorder{samples: make([]time.Duration, 0, hint)}
}

// Observe records a single operation latency.
func (r *Recorder) Observe(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

// Error increments the error counter without recording a latency.
func (r *Recorder) Error() {
	r.mu.Lock()
	r.errors++
	r.mu.Unlock()
}

// Count returns the number of observations recorded so far.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

// Summary snapshots the collected data.
type Summary struct {
	Workload    string        `json:"workload"`
	Concurrency int           `json:"concurrency"`
	OpsTotal    int           `json:"ops_total"`
	Errors      int           `json:"errors"`
	Duration    time.Duration `json:"duration_ns"`
	Throughput  float64       `json:"throughput_ops_per_sec"`
	P50         time.Duration `json:"p50_ns"`
	P90         time.Duration `json:"p90_ns"`
	P99         time.Duration `json:"p99_ns"`
	Max         time.Duration `json:"max_ns"`
}

// Summarize returns a Summary computed over a copy of the collected samples.
func (r *Recorder) Summarize(workload string, concurrency int, wall time.Duration) Summary {
	r.mu.Lock()
	samples := make([]time.Duration, len(r.samples))
	copy(samples, r.samples)
	errs := r.errors
	r.mu.Unlock()

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	s := Summary{
		Workload:    workload,
		Concurrency: concurrency,
		OpsTotal:    len(samples),
		Errors:      errs,
		Duration:    wall,
	}
	if wall > 0 {
		s.Throughput = float64(len(samples)) / wall.Seconds()
	}
	if len(samples) > 0 {
		s.P50 = samples[percentileIndex(len(samples), 0.50)]
		s.P90 = samples[percentileIndex(len(samples), 0.90)]
		s.P99 = samples[percentileIndex(len(samples), 0.99)]
		s.Max = samples[len(samples)-1]
	}
	return s
}

func percentileIndex(n int, p float64) int {
	if n <= 1 {
		return 0
	}
	idx := int(float64(n-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// FormatTable returns a human-readable table of the given summaries in
// the order supplied.
func FormatTable(summaries []Summary) string {
	var b stringBuilder
	b.WriteString("workload             concurrency      ops  errors   thr/s      p50        p99        max\n")
	b.WriteString("-------------------- ----------- -------- ------- -------- ---------- ---------- ----------\n")
	for _, s := range summaries {
		b.WriteString(padRight(s.Workload, 20))
		b.WriteString(" ")
		b.WriteString(padLeft(itoa(s.Concurrency), 11))
		b.WriteString(" ")
		b.WriteString(padLeft(itoa(s.OpsTotal), 8))
		b.WriteString(" ")
		b.WriteString(padLeft(itoa(s.Errors), 7))
		b.WriteString(" ")
		b.WriteString(padLeft(ftoa(s.Throughput, 1), 8))
		b.WriteString(" ")
		b.WriteString(padLeft(s.P50.String(), 10))
		b.WriteString(" ")
		b.WriteString(padLeft(s.P99.String(), 10))
		b.WriteString(" ")
		b.WriteString(padLeft(s.Max.String(), 10))
		b.WriteString("\n")
	}
	return b.String()
}

type stringBuilder struct{ b []byte }

func (s *stringBuilder) WriteString(v string) { s.b = append(s.b, v...) }
func (s *stringBuilder) String() string       { return string(s.b) }

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	out := make([]byte, n)
	copy(out, s)
	for i := len(s); i < n; i++ {
		out[i] = ' '
	}
	return string(out)
}

func padLeft(s string, n int) string {
	if len(s) >= n {
		return s
	}
	out := make([]byte, n)
	for i := 0; i < n-len(s); i++ {
		out[i] = ' '
	}
	copy(out[n-len(s):], s)
	return string(out)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func ftoa(f float64, prec int) string {
	mul := 1.0
	for i := 0; i < prec; i++ {
		mul *= 10
	}
	scaled := int64(f*mul + 0.5)
	if scaled < 0 {
		scaled = 0
	}
	whole := scaled / int64(mul)
	frac := scaled % int64(mul)
	fracStr := itoa(int(frac))
	for len(fracStr) < prec {
		fracStr = "0" + fracStr
	}
	if prec == 0 {
		return itoa(int(whole))
	}
	return itoa(int(whole)) + "." + fracStr
}
