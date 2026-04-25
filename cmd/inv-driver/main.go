// Command inv-driver generates writes for the invalidation bakeoff.
//
// Each subscriber process (inv-logical, inv-notify) measures its own
// write-commit-to-drop-event latency by comparing `updated_at` on the
// received row against the subscriber's wall clock. This keeps the
// driver and subscribers decoupled: the driver just writes, and every
// subscriber can be started / stopped independently.
//
// Usage:
//
//	inv-driver \
//	  -database-url-rw=$DATABASE_URL_RW \
//	  -rate=1000 \
//	  -duration=60s \
//	  -key-space=10000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/keygen"
	"github.com/unkeyed/kv-spike/internal/workload"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "inv-driver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("inv-driver", flag.ExitOnError)
	urls := dbconfig.RegisterFlags(fs)
	rate := fs.Int("rate", 1000, "target writes per second")
	duration := fs.Duration("duration", 60*time.Second, "total run time")
	keySpace := fs.Int("key-space", 10_000, "number of distinct keys to cycle through")
	valueBytes := fs.Int("value-bytes", 256, "value size in bytes")
	workers := fs.Int("workers", 16, "writer goroutines")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rwURL, err := urls.RW()
	if err != nil {
		return err
	}
	pool, err := dbconfig.Open(ctx, rwURL, dbconfig.PoolOptions{
		MinConns:        int32(*workers),
		MaxConns:        int32(*workers * 2),
		MaxConnLifetime: 30 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	fmt.Printf("inv-driver: rate=%d/s duration=%s key-space=%d workers=%d\n",
		*rate, duration.String(), *keySpace, *workers)

	interval := time.Second / time.Duration(*rate)
	if interval <= 0 {
		interval = time.Microsecond
	}

	jobs := make(chan int64, *rate)
	var produced atomic.Int64
	go func() {
		defer close(jobs)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		timeout := time.After(*duration)
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
	var committed atomic.Int64
	var failed atomic.Int64

	layout := keygen.Layout{ValueBytes: *valueBytes}
	value := make([]byte, *valueBytes)
	keygen.RandomValue(value)

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range jobs {
				keyIdx := int(seq) % *keySpace
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
	fmt.Printf("inv-driver done: committed=%d failed=%d target=%d\n",
		committed.Load(), failed.Load(), produced.Load())

	fmt.Println("inv-driver run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}
