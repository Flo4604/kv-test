// Package invdriver runs the write workload that feeds the
// invalidation subscribers. Inserts/updates rows in kv_entries at a
// fixed rate, tagging each row with `updated_at = now_ms` so
// subscribers can compute commit-to-event lag.
package invdriver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/keygen"
	"github.com/unkeyed/kv-spike/internal/workload"
)

// Config controls the driver run.
type Config struct {
	URL        string
	Rate       int           // target writes per second
	Duration   time.Duration // total run time
	KeySpace   int           // distinct keys to cycle through
	ValueBytes int           // value size
	Workers    int           // writer goroutines
}

// Result reports what actually landed.
type Result struct {
	Committed int64
	Failed    int64
	Target    int64
}

// Run blocks until the configured duration elapses or ctx is canceled.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.Rate <= 0 {
		cfg.Rate = 1000
	}
	if cfg.KeySpace <= 0 {
		cfg.KeySpace = 10_000
	}
	if cfg.ValueBytes <= 0 {
		cfg.ValueBytes = 256
	}

	pool, err := dbconfig.Open(ctx, cfg.URL, dbconfig.PoolOptions{
		MinConns:        int32(cfg.Workers),
		MaxConns:        int32(cfg.Workers * 2),
		MaxConnLifetime: 30 * time.Minute,
	})
	if err != nil {
		return Result{}, err
	}
	defer pool.Close()

	interval := time.Second / time.Duration(cfg.Rate)
	if interval <= 0 {
		interval = time.Microsecond
	}

	jobs := make(chan int64, cfg.Rate)
	var produced atomic.Int64
	go func() {
		defer close(jobs)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		timeout := time.After(cfg.Duration)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout:
				return
			case <-ticker.C:
				n := produced.Add(1)
				select {
				case jobs <- n:
				default:
					produced.Add(-1)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	var committed, failed atomic.Int64

	layout := keygen.Layout{ValueBytes: cfg.ValueBytes}
	value := make([]byte, cfg.ValueBytes)
	keygen.RandomValue(value)

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range jobs {
				keyIdx := int(seq) % cfg.KeySpace
				key := layout.Key(keyIdx)
				now := time.Now().UnixMilli()
				_, err := pool.Exec(ctx,
					`INSERT INTO kv_entries
					   (workspace_id, namespace, key, value, metadata, expires_at, updated_at)
					 VALUES ($1, $2, $3, $4, NULL, NULL, $5)
					 ON CONFLICT (workspace_id, namespace, key) DO UPDATE
					   SET value = EXCLUDED.value,
					       updated_at = EXCLUDED.updated_at`,
					workload.Workspace, workload.Namespace, key, value, now,
				)
				if err != nil {
					failed.Add(1)
					continue
				}
				committed.Add(1)
			}
		}()
	}

	wg.Wait()
	return Result{
		Committed: committed.Load(),
		Failed:    failed.Load(),
		Target:    produced.Load(),
	}, nil
}

// Format returns a one-line summary.
func (r Result) Format() string {
	return fmt.Sprintf("driver: committed=%d failed=%d target=%d",
		r.Committed, r.Failed, r.Target)
}
