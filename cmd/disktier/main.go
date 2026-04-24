// Command disktier runs the disk-tier bakeoff for the KV service spike.
//
// Runs a fixed workload (random Get/Put with 1KB values, 100k keys,
// mixed 95/5 at concurrency 1/10/100) against Pebble, Badger, and
// BoltDB. Reports p50/p99 latency, throughput, on-disk size, and
// time-to-reopen per engine. Emits both a stdout table and a JSON
// summary.
//
// Usage:
//
//	disktier \
//	  -dir=/var/tmp/kv-bakeoff \
//	  -keys=100000 \
//	  -duration=15s \
//	  -concurrencies=1,10,100 \
//	  -output=disktier_results.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v4"
	bolt "go.etcd.io/bbolt"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/keygen"
)

type engineFactory struct {
	name string
	open func(dir string) (engine, error)
}

type engine interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Close() error
	Name() string
}

type engineResult struct {
	Engine         string               `json:"engine"`
	Workloads      []benchstats.Summary `json:"workloads"`
	OnDiskBytes    int64                `json:"on_disk_bytes"`
	TimeToReopenMs int64                `json:"time_to_reopen_ms"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "disktier: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("disktier", flag.ExitOnError)
	dir := fs.String("dir", filepath.Join(os.TempDir(), "kv-disktier-bakeoff"), "base directory for engine data")
	keys := fs.Int("keys", 100_000, "number of distinct keys")
	valueBytes := fs.Int("value-bytes", 1024, "value size")
	duration := fs.Duration("duration", 10*time.Second, "per-workload run duration")
	warmup := fs.Duration("warmup", 2*time.Second, "per-workload warmup duration")
	concurrenciesStr := fs.String("concurrencies", "1,10,100", "comma-separated concurrency list")
	output := fs.String("output", "disktier_results.json", "JSON output path")
	enginesStr := fs.String("engines", "pebble,badger,bolt", "comma-separated engines to run")
	keepData := fs.Bool("keep-data", false, "retain data dirs after run (default: rm)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	concurrencies, err := parseInts(*concurrenciesStr)
	if err != nil {
		return err
	}
	wantEngines := commaSet(*enginesStr)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}

	factories := []engineFactory{
		{name: "pebble", open: openPebble},
		{name: "badger", open: openBadger},
		{name: "bolt", open: openBolt},
	}

	layout := keygen.Layout{ValueBytes: *valueBytes, KeysEach: *keys}

	results := []engineResult{}
	for _, f := range factories {
		if _, ok := wantEngines[f.name]; !ok {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		engDir := filepath.Join(*dir, f.name)
		_ = os.RemoveAll(engDir)
		if err := os.MkdirAll(engDir, 0o755); err != nil {
			return err
		}
		fmt.Printf("== engine: %s (dir=%s) ==\n", f.name, engDir)
		res, err := runEngine(ctx, f, engDir, layout, concurrencies, *duration, *warmup)
		if err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
		results = append(results, res)
		if !*keepData {
			_ = os.RemoveAll(engDir)
		}
	}

	fmt.Println()
	for _, r := range results {
		fmt.Printf("engine=%s on_disk_bytes=%d reopen_ms=%d\n", r.Engine, r.OnDiskBytes, r.TimeToReopenMs)
		fmt.Println(benchstats.FormatTable(r.Workloads))
	}

	return writeJSON(*output, results)
}

func runEngine(
	ctx context.Context,
	f engineFactory,
	dir string,
	layout keygen.Layout,
	concurrencies []int,
	duration, warmup time.Duration,
) (engineResult, error) {
	eng, err := f.open(dir)
	if err != nil {
		return engineResult{}, fmt.Errorf("open: %w", err)
	}

	fmt.Printf("  seeding %d keys x %d bytes\n", layout.KeysEach, layout.ValueBytes)
	if err := seedEngine(eng, layout); err != nil {
		_ = eng.Close()
		return engineResult{}, fmt.Errorf("seed: %w", err)
	}

	var summaries []benchstats.Summary
	workloads := []struct {
		name string
		fn   func(ctx context.Context, w *engWorker) error
	}{
		{"Get", engWorkloadGet},
		{"Put", engWorkloadPut(layout.ValueBytes)},
		{"Mixed_95_5", engWorkloadMixed(layout.ValueBytes, 0.95)},
	}
	for _, wl := range workloads {
		for _, c := range concurrencies {
			if ctx.Err() != nil {
				break
			}
			fmt.Printf("  workload=%s concurrency=%d\n", wl.name, c)
			s := driveEngine(ctx, eng, layout, wl.name, wl.fn, c, warmup, duration)
			summaries = append(summaries, s)
		}
	}

	if err := eng.Close(); err != nil {
		return engineResult{}, fmt.Errorf("close: %w", err)
	}

	size, err := dirSize(dir)
	if err != nil {
		return engineResult{}, fmt.Errorf("dir size: %w", err)
	}

	reopenStart := time.Now()
	eng2, err := f.open(dir)
	if err != nil {
		return engineResult{}, fmt.Errorf("reopen: %w", err)
	}
	reopenMs := time.Since(reopenStart).Milliseconds()
	if err := eng2.Close(); err != nil {
		return engineResult{}, fmt.Errorf("close after reopen: %w", err)
	}

	return engineResult{
		Engine:         f.name,
		Workloads:      summaries,
		OnDiskBytes:    size,
		TimeToReopenMs: reopenMs,
	}, nil
}

type engWorker struct {
	eng    engine
	layout keygen.Layout
	rng    *mrand.Rand
	buf    []byte
}

func driveEngine(
	ctx context.Context,
	eng engine,
	layout keygen.Layout,
	name string,
	fn func(ctx context.Context, w *engWorker) error,
	concurrency int,
	warmup, duration time.Duration,
) benchstats.Summary {
	{
		warmupCtx, cancel := context.WithTimeout(ctx, warmup)
		driveOnce(warmupCtx, eng, layout, fn, concurrency)
		cancel()
	}

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rec := driveOnce(runCtx, eng, layout, fn, concurrency)
	return rec.Summarize(fmt.Sprintf("%s_c%d", name, concurrency), concurrency, duration)
}

func driveOnce(
	ctx context.Context,
	eng engine,
	layout keygen.Layout,
	fn func(ctx context.Context, w *engWorker) error,
	concurrency int,
) *benchstats.Recorder {
	rec := benchstats.NewRecorder(8192)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			w := &engWorker{
				eng:    eng,
				layout: layout,
				rng:    mrand.New(mrand.NewPCG(seed, seed^0x51bd4f3e)),
				buf:    make([]byte, layout.ValueBytes),
			}
			for ctx.Err() == nil {
				start := time.Now()
				err := fn(ctx, w)
				elapsed := time.Since(start)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					rec.Error()
					continue
				}
				rec.Observe(elapsed)
			}
		}(uint64(i + 1))
	}
	wg.Wait()
	return rec
}

func engWorkloadGet(ctx context.Context, w *engWorker) error {
	k := []byte(w.layout.Key(w.rng.IntN(w.layout.KeysEach)))
	_, err := w.eng.Get(k)
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}

func engWorkloadPut(valBytes int) func(ctx context.Context, w *engWorker) error {
	return func(ctx context.Context, w *engWorker) error {
		k := []byte(w.layout.Key(w.rng.IntN(w.layout.KeysEach)))
		keygen.RandomValue(w.buf)
		return w.eng.Put(k, w.buf)
	}
}

func engWorkloadMixed(valBytes int, readFraction float64) func(ctx context.Context, w *engWorker) error {
	put := engWorkloadPut(valBytes)
	return func(ctx context.Context, w *engWorker) error {
		if w.rng.Float64() < readFraction {
			return engWorkloadGet(ctx, w)
		}
		return put(ctx, w)
	}
}

func seedEngine(eng engine, layout keygen.Layout) error {
	buf := make([]byte, layout.ValueBytes)
	for i := 0; i < layout.KeysEach; i++ {
		keygen.RandomValue(buf)
		if err := eng.Put([]byte(layout.Key(i)), buf); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------
// engines
// ---------------------------------------------------------------

var errNotFound = errors.New("not found")

type pebbleEngine struct {
	db *pebble.DB
}

func openPebble(dir string) (engine, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &pebbleEngine{db: db}, nil
}
func (p *pebbleEngine) Name() string { return "pebble" }
func (p *pebbleEngine) Get(key []byte) ([]byte, error) {
	val, closer, err := p.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, errNotFound
		}
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, closer.Close()
}
func (p *pebbleEngine) Put(key, value []byte) error {
	return p.db.Set(key, value, pebble.Sync)
}
func (p *pebbleEngine) Close() error { return p.db.Close() }

type badgerEngine struct {
	db *badger.DB
}

func openBadger(dir string) (engine, error) {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerEngine{db: db}, nil
}
func (b *badgerEngine) Name() string { return "badger" }
func (b *badgerEngine) Get(key []byte) ([]byte, error) {
	var out []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = make([]byte, len(val))
			copy(out, val)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, errNotFound
	}
	return out, err
}
func (b *badgerEngine) Put(key, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}
func (b *badgerEngine) Close() error { return b.db.Close() }

var boltBucket = []byte("kv")

type boltEngine struct {
	db *bolt.DB
}

func openBolt(dir string) (engine, error) {
	db, err := bolt.Open(filepath.Join(dir, "kv.bolt"), 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &boltEngine{db: db}, nil
}
func (b *boltEngine) Name() string { return "bolt" }
func (b *boltEngine) Get(key []byte) ([]byte, error) {
	var out []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(boltBucket).Get(key)
		if v == nil {
			return errNotFound
		}
		out = make([]byte, len(v))
		copy(out, v)
		return nil
	})
	return out, err
}
func (b *boltEngine) Put(key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Put(key, value)
	})
}
func (b *boltEngine) Close() error { return b.db.Close() }

// ---------------------------------------------------------------
// helpers
// ---------------------------------------------------------------

func dirSize(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func parseInts(csv string) ([]int, error) {
	if csv == "" {
		return nil, errors.New("empty concurrency list")
	}
	out := make([]int, 0, 4)
	cur := 0
	have := false
	for i := 0; i < len(csv); i++ {
		c := csv[i]
		switch {
		case c == ',':
			if !have {
				return nil, fmt.Errorf("bad list: %q", csv)
			}
			out = append(out, cur)
			cur = 0
			have = false
		case c >= '0' && c <= '9':
			cur = cur*10 + int(c-'0')
			have = true
		case c == ' ':
			continue
		default:
			return nil, fmt.Errorf("bad character %q in list", string(c))
		}
	}
	if !have {
		return nil, fmt.Errorf("bad list: %q", csv)
	}
	out = append(out, cur)
	return out, nil
}

func commaSet(csv string) map[string]struct{} {
	out := map[string]struct{}{}
	start := 0
	for i := 0; i <= len(csv); i++ {
		if i == len(csv) || csv[i] == ',' {
			name := csv[start:i]
			if name != "" {
				out[name] = struct{}{}
			}
			start = i + 1
		}
	}
	return out
}

func writeJSON(path string, results []engineResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Timestamp string         `json:"timestamp"`
		Results   []engineResult `json:"results"`
	}{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Results:   results,
	})
}
