// Command bench is the Postgres latency bench for the KV service spike.
//
// Subcommands:
//
//	seed    Populate the kv_entries corpus (10 workspaces, 10 ns, 100k keys, 1KB).
//	bench   Run Get / GetMany(100) / Put / Batch(5) / mixed(95/5) at 1/10/100 concurrency.
//
// Usage:
//
//	bench seed   -database-url=postgresql://...
//	bench bench  -database-url-ro=... -database-url-rw=...
//
// Results: p50/p99/throughput per workload printed to stdout plus a
// machine-readable JSON summary written to bench_results.json.
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/keygen"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var err error
	switch sub {
	case "seed":
		err = runSeed(ctx, args)
	case "bench":
		err = runBench(ctx, args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", sub)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench <seed|bench> [flags]")
	fmt.Fprintln(os.Stderr, "  seed    populate kv_entries with the default corpus")
	fmt.Fprintln(os.Stderr, "  bench   run the full workload matrix")
}

// ---------------------------------------------------------------
// seed
// ---------------------------------------------------------------

func runSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	urls := dbconfig.RegisterFlags(fs)
	workspaces := fs.Int("workspaces", 10, "number of workspaces")
	namespaces := fs.Int("namespaces", 10, "namespaces per workspace")
	keys := fs.Int("keys", 100_000, "keys per (workspace, namespace)")
	valueBytes := fs.Int("value-bytes", 1024, "size of each value in bytes")
	batchSize := fs.Int("batch-size", 1000, "rows per INSERT batch")
	parallelism := fs.Int("parallelism", 8, "concurrent insert workers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	layout := keygen.Layout{
		Workspaces:     *workspaces,
		NamespacesEach: *namespaces,
		KeysEach:       *keys,
		ValueBytes:     *valueBytes,
	}
	rwURL, err := urls.RW()
	if err != nil {
		return err
	}
	pool, err := dbconfig.Open(ctx, rwURL, dbconfig.PoolOptions{
		MinConns:        int32(*parallelism),
		MaxConns:        int32(*parallelism * 2),
		MaxConnLifetime: 30 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	total := layout.TotalKeys()
	fmt.Printf("seeding %d rows (%d workspaces x %d namespaces x %d keys x %d bytes) via %d workers\n",
		total, layout.Workspaces, layout.NamespacesEach, layout.KeysEach, layout.ValueBytes, *parallelism)

	type task struct {
		workspace int
		namespace int
	}
	tasks := make(chan task)
	errCh := make(chan error, *parallelism)
	var inserted int64
	var mu sync.Mutex
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *parallelism; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				if ctx.Err() != nil {
					return
				}
				if err := seedNamespace(ctx, pool, layout, t.workspace, t.namespace, *batchSize); err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				inserted += int64(layout.KeysEach)
				done := inserted
				mu.Unlock()
				fmt.Printf("  progress: %d / %d rows (%.1f%%)\n", done, total, 100*float64(done)/float64(total))
			}
		}()
	}

	go func() {
		for w := 0; w < layout.Workspaces; w++ {
			for n := 0; n < layout.NamespacesEach; n++ {
				select {
				case <-ctx.Done():
					close(tasks)
					return
				case tasks <- task{workspace: w, namespace: n}:
				}
			}
		}
		close(tasks)
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("seed done in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

func seedNamespace(ctx context.Context, pool *pgxpool.Pool, layout keygen.Layout, w, n, batchSize int) error {
	workspace := layout.Workspace(w)
	namespace := layout.Namespace(n)
	now := time.Now().UnixMilli()

	for offset := 0; offset < layout.KeysEach; offset += batchSize {
		end := offset + batchSize
		if end > layout.KeysEach {
			end = layout.KeysEach
		}
		batch := &pgx.Batch{}
		for k := offset; k < end; k++ {
			seed := uint64(w)*1_000_000_000 + uint64(n)*1_000_000 + uint64(k)
			batch.Queue(
				`INSERT INTO kv_entries
				  (workspace_id, namespace, key, value, metadata, expires_at, updated_at)
				 VALUES ($1, $2, $3, $4, NULL, NULL, $5)
				 ON CONFLICT (workspace_id, namespace, key) DO UPDATE
				   SET value = EXCLUDED.value,
				       updated_at = EXCLUDED.updated_at`,
				workspace, namespace, layout.Key(k), layout.Value(seed), now,
			)
		}
		br := pool.SendBatch(ctx, batch)
		for i := offset; i < end; i++ {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return fmt.Errorf("seed %s/%s/%d: %w", workspace, namespace, i, err)
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------
// bench
// ---------------------------------------------------------------

type workloadFn func(ctx context.Context, w *worker) error

type worker struct {
	roPool *pgxpool.Pool
	rwPool *pgxpool.Pool
	layout keygen.Layout
	rng    *mrand.Rand
}

type workload struct {
	name string
	fn   workloadFn
}

func runBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	urls := dbconfig.RegisterFlags(fs)
	durationFlag := fs.Duration("duration", 10*time.Second, "per-workload run duration")
	warmup := fs.Duration("warmup", 2*time.Second, "per-workload warmup duration")
	maxConns := fs.Int("max-conns", 128, "max pool size")
	output := fs.String("output", "bench_results.json", "machine-readable results path")
	concurrenciesStr := fs.String("concurrencies", "1,10,100", "comma-separated concurrency levels")
	workloadsFilter := fs.String("workloads", "", "comma-separated workload names to run (empty = all). options: Get,GetHot,GetMany_100,Put,Batch_5,Mixed_95_5")
	hotKeys := fs.Int("hot-keys", 100, "key-space size for GetHot: reads are randomly drawn from the first N keys of (ws_000, ns_000) so the plan cache and buffer pool stay warm")
	loop := fs.Bool("loop", false, "repeat the workload matrix forever instead of exiting after one run")
	loopInterval := fs.Duration("loop-interval", 30*time.Second, "sleep between iterations when -loop is set")
	if err := fs.Parse(args); err != nil {
		return err
	}
	concurrencies, err := parseInts(*concurrenciesStr)
	if err != nil {
		return err
	}

	roURL, err := urls.RO()
	if err != nil {
		return err
	}
	rwURL, err := urls.RW()
	if err != nil {
		return err
	}

	roPool, err := dbconfig.Open(ctx, roURL, dbconfig.PoolOptions{
		MinConns:        int32(maxInt(1, *maxConns/4)),
		MaxConns:        int32(*maxConns),
		MaxConnLifetime: 30 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("open RO pool: %w", err)
	}
	defer roPool.Close()

	var rwPool *pgxpool.Pool
	if rwURL == roURL {
		rwPool = roPool
	} else {
		rwPool, err = dbconfig.Open(ctx, rwURL, dbconfig.PoolOptions{
			MinConns:        int32(maxInt(1, *maxConns/4)),
			MaxConns:        int32(*maxConns),
			MaxConnLifetime: 30 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("open RW pool: %w", err)
		}
		defer rwPool.Close()
	}

	layout := keygen.Default()

	allWorkloads := []workload{
		{name: "Get", fn: workloadGet},
		{name: "GetHot", fn: workloadGetHot(*hotKeys)},
		{name: "GetMany_100", fn: workloadGetMany(100)},
		{name: "Put", fn: workloadPut},
		{name: "Batch_5", fn: workloadBatch(5)},
		{name: "Mixed_95_5", fn: workloadMixed(0.95)},
	}
	workloads, err := filterWorkloads(allWorkloads, *workloadsFilter)
	if err != nil {
		return err
	}

	iteration := 0
	for {
		iteration++
		if *loop {
			fmt.Printf("\n=== iteration %d (loop=true, interval=%s) ===\n", iteration, loopInterval.String())
		}

		summaries := make([]benchstats.Summary, 0, len(workloads)*len(concurrencies))
		for _, wl := range workloads {
			for _, c := range concurrencies {
				if ctx.Err() != nil {
					return nil
				}
				fmt.Printf("> workload=%s concurrency=%d warmup=%s duration=%s\n",
					wl.name, c, warmup.String(), durationFlag.String())
				s, runErr := runWorkload(ctx, roPool, rwPool, layout, wl, c, *warmup, *durationFlag)
				if runErr != nil {
					return fmt.Errorf("workload %s c=%d: %w", wl.name, c, runErr)
				}
				summaries = append(summaries, s)
			}
		}

		fmt.Println()
		fmt.Println(benchstats.FormatTable(summaries))

		if err := writeJSON(*output, summaries); err != nil {
			return fmt.Errorf("write results: %w", err)
		}
		fmt.Printf("wrote %s\n", *output)

		if !*loop {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*loopInterval):
		}
	}

	// Keep the container alive so auto-restart loops don't churn. The
	// operator exits with SIGTERM / SIGINT.
	fmt.Println()
	fmt.Println("bench run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}

func filterWorkloads(all []workload, filter string) ([]workload, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return all, nil
	}
	want := map[string]struct{}{}
	for _, name := range strings.Split(filter, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		want[name] = struct{}{}
	}
	known := map[string]struct{}{}
	for _, wl := range all {
		known[wl.name] = struct{}{}
	}
	for name := range want {
		if _, ok := known[name]; !ok {
			return nil, fmt.Errorf("unknown workload %q (known: Get, GetHot, GetMany_100, Put, Batch_5, Mixed_95_5)", name)
		}
	}
	out := make([]workload, 0, len(want))
	for _, wl := range all {
		if _, ok := want[wl.name]; ok {
			out = append(out, wl)
		}
	}
	return out, nil
}

func runWorkload(
	ctx context.Context,
	roPool, rwPool *pgxpool.Pool,
	layout keygen.Layout,
	wl workload,
	concurrency int,
	warmup, duration time.Duration,
) (benchstats.Summary, error) {
	warmupCtx, cancel := context.WithTimeout(ctx, warmup)
	_, _ = driveWorkload(warmupCtx, roPool, rwPool, layout, wl, concurrency)
	cancel()

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rec, err := driveWorkload(runCtx, roPool, rwPool, layout, wl, concurrency)
	if err != nil {
		return benchstats.Summary{}, err
	}
	return rec.Summarize(fmt.Sprintf("%s_c%d", wl.name, concurrency), concurrency, duration), nil
}

func driveWorkload(
	ctx context.Context,
	roPool, rwPool *pgxpool.Pool,
	layout keygen.Layout,
	wl workload,
	concurrency int,
) (*benchstats.Recorder, error) {
	rec := benchstats.NewRecorder(8192)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			w := &worker{
				roPool: roPool,
				rwPool: rwPool,
				layout: layout,
				rng:    mrand.New(mrand.NewPCG(seed, seed^0xdeadbeef)),
			}
			for ctx.Err() == nil {
				start := time.Now()
				err := wl.fn(ctx, w)
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
	return rec, nil
}

func workloadGet(ctx context.Context, w *worker) error {
	ws, ns, key := w.layout.RandomTuple(w.rng)
	var value []byte
	err := w.roPool.QueryRow(ctx,
		`SELECT value FROM kv_entries WHERE workspace_id = $1 AND namespace = $2 AND key = $3`,
		ws, ns, key,
	).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}

// workloadGetHot reads from a fixed hotspot in (ws_000, ns_000) over a
// small key window. Compared to workloadGet it keeps PG's plan cache
// and buffer pool hot, so the numbers reflect steady-state read
// latency on rows that are already in cache. The normal Get workload
// (random across 10M keys) reflects cold-read latency instead.
func workloadGetHot(hotKeys int) workloadFn {
	if hotKeys <= 0 {
		hotKeys = 100
	}
	return func(ctx context.Context, w *worker) error {
		ws := w.layout.Workspace(0)
		ns := w.layout.Namespace(0)
		key := w.layout.Key(w.rng.IntN(hotKeys))
		var value []byte
		err := w.roPool.QueryRow(ctx,
			`SELECT value FROM kv_entries WHERE workspace_id = $1 AND namespace = $2 AND key = $3`,
			ws, ns, key,
		).Scan(&value)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
}

func workloadGetMany(n int) workloadFn {
	return func(ctx context.Context, w *worker) error {
		ws := w.layout.Workspace(w.rng.IntN(w.layout.Workspaces))
		ns := w.layout.Namespace(w.rng.IntN(w.layout.NamespacesEach))
		keys := make([]string, n)
		for i := range keys {
			keys[i] = w.layout.Key(w.rng.IntN(w.layout.KeysEach))
		}
		rows, err := w.roPool.Query(ctx,
			`SELECT key, value FROM kv_entries
			  WHERE workspace_id = $1 AND namespace = $2 AND key = ANY($3)`,
			ws, ns, keys,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k string
			var v []byte
			if err := rows.Scan(&k, &v); err != nil {
				return err
			}
		}
		return rows.Err()
	}
}

func workloadPut(ctx context.Context, w *worker) error {
	ws, ns, key := w.layout.RandomTuple(w.rng)
	value := make([]byte, w.layout.ValueBytes)
	keygen.RandomValue(value)
	now := time.Now().UnixMilli()
	_, err := w.rwPool.Exec(ctx,
		`INSERT INTO kv_entries (workspace_id, namespace, key, value, metadata, expires_at, updated_at)
		 VALUES ($1, $2, $3, $4, NULL, NULL, $5)
		 ON CONFLICT (workspace_id, namespace, key) DO UPDATE
		   SET value = EXCLUDED.value,
		       updated_at = EXCLUDED.updated_at`,
		ws, ns, key, value, now,
	)
	return err
}

func workloadBatch(n int) workloadFn {
	return func(ctx context.Context, w *worker) error {
		ws := w.layout.Workspace(w.rng.IntN(w.layout.Workspaces))
		ns := w.layout.Namespace(w.rng.IntN(w.layout.NamespacesEach))
		tx, err := w.rwPool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		now := time.Now().UnixMilli()
		value := make([]byte, w.layout.ValueBytes)
		for i := 0; i < n; i++ {
			keygen.RandomValue(value)
			key := w.layout.Key(w.rng.IntN(w.layout.KeysEach))
			_, err := tx.Exec(ctx,
				`INSERT INTO kv_entries (workspace_id, namespace, key, value, metadata, expires_at, updated_at)
				 VALUES ($1, $2, $3, $4, NULL, NULL, $5)
				 ON CONFLICT (workspace_id, namespace, key) DO UPDATE
				   SET value = EXCLUDED.value,
				       updated_at = EXCLUDED.updated_at`,
				ws, ns, key, value, now,
			)
			if err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
		}
		return tx.Commit(ctx)
	}
}

func workloadMixed(readFraction float64) workloadFn {
	return func(ctx context.Context, w *worker) error {
		if w.rng.Float64() < readFraction {
			return workloadGet(ctx, w)
		}
		return workloadPut(ctx, w)
	}
}

// ---------------------------------------------------------------
// helpers
// ---------------------------------------------------------------

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
				return nil, fmt.Errorf("bad concurrency list: %q", csv)
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
			return nil, fmt.Errorf("bad character %q in concurrency list", string(c))
		}
	}
	if !have {
		return nil, fmt.Errorf("bad concurrency list: %q", csv)
	}
	out = append(out, cur)
	return out, nil
}

func writeJSON(path string, summaries []benchstats.Summary) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	payload := struct {
		Timestamp string               `json:"timestamp"`
		Results   []benchstats.Summary `json:"results"`
	}{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Results:   summaries,
	}
	return enc.Encode(payload)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
