// Package invnotify is the LISTEN/NOTIFY subscriber for the
// invalidation bakeoff.
package invnotify

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/workload"
)

// Config controls a notify-subscriber run.
type Config struct {
	URL           string
	Duration      time.Duration
	SleepAfter    time.Duration // 0 disables the reconnect test
	SleepDuration time.Duration
}

// Result reports the subscriber's collected lag plus the missed-
// window count produced by any reconnect test.
type Result struct {
	Summary       benchstats.Summary
	MissedWindows int
}

// Run blocks until ctx is canceled or the configured duration elapses.
func Run(ctx context.Context, cfg Config) (Result, error) {
	pool, err := dbconfig.Open(ctx, cfg.URL, dbconfig.PoolOptions{
		MinConns: 1,
		MaxConns: 4,
	})
	if err != nil {
		return Result{}, err
	}
	defer pool.Close()

	rec := benchstats.NewRecorder(1024)
	var missed int

	deadline := time.Now().Add(cfg.Duration)

	for time.Now().Before(deadline) && ctx.Err() == nil {
		sessionCtx, sessionCancel := context.WithDeadline(ctx, deadline)
		if cfg.SleepAfter > 0 {
			var cancel2 context.CancelFunc
			sessionCtx, cancel2 = context.WithTimeout(sessionCtx, cfg.SleepAfter)
			sessionCancel = func() {
				cancel2()
				sessionCancel()
			}
		}

		conn, err := pool.Acquire(sessionCtx)
		if err != nil {
			sessionCancel()
			if ctx.Err() != nil {
				break
			}
			return Result{}, fmt.Errorf("acquire listen conn: %w", err)
		}
		if _, err := conn.Exec(sessionCtx, "LISTEN kv_invalidate"); err != nil {
			conn.Release()
			sessionCancel()
			return Result{}, fmt.Errorf("LISTEN: %w", err)
		}
		if err := pumpNotifies(sessionCtx, conn.Conn(), pool, rec); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				conn.Release()
				sessionCancel()
				return Result{}, err
			}
		}
		conn.Release()
		sessionCancel()

		if cfg.SleepAfter > 0 && time.Now().Before(deadline) {
			sleepFor := cfg.SleepDuration
			if until := time.Until(deadline); until < sleepFor {
				sleepFor = until
			}
			if sleepFor > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(sleepFor):
				}
				cfg.SleepAfter = 0
				missed++
			}
		} else {
			break
		}
	}

	summary := rec.Summarize("notify_lag", 1, cfg.Duration)
	return Result{Summary: summary, MissedWindows: missed}, nil
}

func pumpNotifies(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool, rec *benchstats.Recorder) error {
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		now := time.Now().UnixMilli()
		ws, ns, key, perr := workload.DecodeNotifyPayload(n.Payload)
		if perr != nil {
			rec.Error()
			continue
		}
		var updatedAt int64
		err = pool.QueryRow(ctx,
			`SELECT updated_at FROM kv_entries
			 WHERE workspace_id = $1 AND namespace = $2 AND key = $3`,
			ws, ns, key,
		).Scan(&updatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			rec.Error()
			continue
		}
		lag := time.Duration(now-updatedAt) * time.Millisecond
		if lag < 0 {
			lag = 0
		}
		rec.Observe(lag)
	}
}
