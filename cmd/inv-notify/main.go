// Command inv-notify is the LISTEN/NOTIFY subscriber for the
// invalidation bakeoff.
//
// On each NOTIFY it parses the (workspace, namespace, key) tuple,
// reads the row's updated_at via a follow-up SELECT on the RO pool,
// and records (now - updated_at) as the end-to-end lag. Results are
// written as JSON to -output.
//
// Reconnect behavior: if the -sleep-after flag is set, the subscriber
// closes its LISTEN connection after that duration and reconnects
// after -sleep-duration. Events produced during the gap are lost.
//
// Usage:
//
//	inv-notify \
//	  -database-url=$DATABASE_URL_RO \
//	  -duration=60s \
//	  -output=notify_lag.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/workload"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "inv-notify: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("inv-notify", flag.ExitOnError)
	urls := dbconfig.RegisterFlags(fs)
	duration := fs.Duration("duration", 60*time.Second, "total run time")
	sleepAfter := fs.Duration("sleep-after", 0, "if >0, close the LISTEN connection after this long to simulate a cut")
	sleepDuration := fs.Duration("sleep-duration", 30*time.Second, "how long to stay disconnected before reconnecting")
	output := fs.String("output", "notify_lag.json", "JSON results path")
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
		MinConns: 1,
		MaxConns: 4,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	rec := benchstats.NewRecorder(1024)
	var missed int

	fmt.Printf("inv-notify: listening for %s (sleep-after=%s sleep-duration=%s)\n",
		duration.String(), sleepAfter.String(), sleepDuration.String())

	deadline := time.Now().Add(*duration)

	for time.Now().Before(deadline) && ctx.Err() == nil {
		sessionCtx, sessionCancel := context.WithDeadline(ctx, deadline)
		if *sleepAfter > 0 {
			var cancel2 context.CancelFunc
			sessionCtx, cancel2 = context.WithTimeout(sessionCtx, *sleepAfter)
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
			return fmt.Errorf("acquire listen conn: %w", err)
		}
		if _, err := conn.Exec(sessionCtx, "LISTEN kv_invalidate"); err != nil {
			conn.Release()
			sessionCancel()
			return fmt.Errorf("LISTEN: %w", err)
		}
		if err := pumpNotifies(sessionCtx, conn.Conn(), pool, rec); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				conn.Release()
				sessionCancel()
				return err
			}
		}
		conn.Release()
		sessionCancel()

		if *sleepAfter > 0 && time.Now().Before(deadline) {
			sleepFor := *sleepDuration
			if until := time.Until(deadline); until < sleepFor {
				sleepFor = until
			}
			if sleepFor > 0 {
				fmt.Printf("inv-notify: simulating %s disconnect (events during this window are LOST)\n",
					sleepFor.String())
				missedStart := time.Now()
				select {
				case <-ctx.Done():
				case <-time.After(sleepFor):
				}
				fmt.Printf("inv-notify: reconnecting after %s\n", time.Since(missedStart).Round(time.Millisecond))
				*sleepAfter = 0
				missed++
			}
		} else {
			break
		}
	}

	summary := rec.Summarize("notify_lag", 1, *duration)
	summary.Workload = fmt.Sprintf("notify_lag (missed windows=%d)", missed)

	fmt.Println()
	fmt.Println(benchstats.FormatTable([]benchstats.Summary{summary}))
	if missed > 0 {
		fmt.Println("NOTE: events produced during the disconnect window are permanently lost.")
		fmt.Println("      An anti-entropy sweep is required to make LISTEN/NOTIFY viable.")
	}
	if err := writeJSON(*output, summary, missed); err != nil {
		return err
	}

	fmt.Println("inv-notify run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
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

func writeJSON(path string, s benchstats.Summary, missed int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Summary          benchstats.Summary `json:"summary"`
		MissedWindows    int                `json:"missed_reconnect_windows"`
		BackfillProperty string             `json:"backfill_property"`
	}{
		Summary:          s,
		MissedWindows:    missed,
		BackfillProperty: "LISTEN/NOTIFY has no server-side backlog. Events produced while the subscriber is disconnected are lost. An anti-entropy sweep is required.",
	})
}
