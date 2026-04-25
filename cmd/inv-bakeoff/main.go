// Command inv-bakeoff runs the full invalidation bakeoff in one
// process: it starts the logical-replication subscriber and the
// LISTEN/NOTIFY subscriber, waits for them to attach, runs the
// driver, drains the subscribers, prints both summaries, and stays
// alive afterwards.
//
// Single deploy. Single command. One log stream with both results.
//
// Usage:
//
//	inv-bakeoff \
//	  -database-url=$DATABASE_URL_RW \
//	  -driver-duration=60s \
//	  -driver-rate=1000
//
// Reconnect test:
//
//	inv-bakeoff -reconnect -driver-duration=120s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/invdriver"
	"github.com/unkeyed/kv-spike/internal/invlogical"
	"github.com/unkeyed/kv-spike/internal/invnotify"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "inv-bakeoff: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("inv-bakeoff", flag.ExitOnError)
	urls := dbconfig.RegisterFlags(fs)

	driverDuration := fs.Duration("driver-duration", 60*time.Second, "how long the writer runs")
	driverRate := fs.Int("driver-rate", 1000, "writes per second target")
	driverWorkers := fs.Int("driver-workers", 8, "writer goroutines")
	driverKeySpace := fs.Int("driver-key-space", 10_000, "distinct keys to cycle through")
	driverValueBytes := fs.Int("driver-value-bytes", 256, "value size")

	slot := fs.String("slot", "kv_inv_bakeoff", "logical replication slot name")
	publication := fs.String("publication", "kv_invalidate_pub", "publication on the primary")

	attachWait := fs.Duration("attach-wait", 3*time.Second, "wait for subscribers to attach before starting the driver")
	drainWait := fs.Duration("drain-wait", 5*time.Second, "wait for subscribers to drain after the driver stops")

	reconnect := fs.Bool("reconnect", false, "run the reconnect test (subscribers cut and resume)")
	sleepAfter := fs.Duration("reconnect-after", 30*time.Second, "subscribers cut after this long when -reconnect")
	sleepDuration := fs.Duration("reconnect-duration", 30*time.Second, "subscribers stay disconnected this long")

	cleanupSlot := fs.Bool("cleanup-slot", false, "drop the logical replication slot after the run")

	output := fs.String("output", "bakeoff_results.json", "machine-readable JSON results path")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rwURL, err := urls.RW()
	if err != nil {
		return err
	}

	// Compute total subscriber duration: must outlive attach + driver + drain.
	subDuration := *attachWait + *driverDuration + *drainWait
	if *reconnect {
		// Reconnect test consumes one sleep cycle inside the subscriber's own loop.
		subDuration = *attachWait + *driverDuration + *drainWait
	}

	fmt.Printf("inv-bakeoff: driver=%s @ %d/s; subscribers=%s; reconnect=%v\n",
		driverDuration.String(), *driverRate, subDuration.String(), *reconnect)

	// Pre-create the slot so the writer does not race with slot creation.
	fmt.Printf("ensuring replication slot %q\n", *slot)
	if err := invlogical.EnsureSlot(ctx, rwURL, *slot); err != nil {
		return fmt.Errorf("ensure slot: %w", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	logicalCfg := invlogical.Config{
		URL:         rwURL,
		Slot:        *slot,
		Publication: *publication,
		Duration:    subDuration,
	}
	notifyCfg := invnotify.Config{
		URL:      rwURL,
		Duration: subDuration,
	}
	if *reconnect {
		logicalCfg.SleepAfter = *sleepAfter
		logicalCfg.SleepDuration = *sleepDuration
		notifyCfg.SleepAfter = *sleepAfter
		notifyCfg.SleepDuration = *sleepDuration
	}

	var (
		wg         sync.WaitGroup
		logicalRes invlogical.Result
		notifyRes  invnotify.Result
		logicalErr error
		notifyErr  error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		logicalRes, logicalErr = invlogical.Run(subCtx, logicalCfg)
	}()
	go func() {
		defer wg.Done()
		notifyRes, notifyErr = invnotify.Run(subCtx, notifyCfg)
	}()

	// Give subscribers a moment to attach before flooding writes.
	fmt.Printf("waiting %s for subscribers to attach\n", attachWait.String())
	select {
	case <-ctx.Done():
		subCancel()
		wg.Wait()
		return ctx.Err()
	case <-time.After(*attachWait):
	}

	fmt.Printf("starting driver: %d/s for %s\n", *driverRate, driverDuration.String())
	driverRes, err := invdriver.Run(ctx, invdriver.Config{
		URL:        rwURL,
		Rate:       *driverRate,
		Duration:   *driverDuration,
		KeySpace:   *driverKeySpace,
		ValueBytes: *driverValueBytes,
		Workers:    *driverWorkers,
	})
	if err != nil {
		subCancel()
		wg.Wait()
		return fmt.Errorf("driver: %w", err)
	}
	fmt.Println(driverRes.Format())

	// Let subscribers drain in-flight events before tearing down.
	fmt.Printf("draining subscribers for %s\n", drainWait.String())
	select {
	case <-ctx.Done():
	case <-time.After(*drainWait):
	}

	subCancel()
	wg.Wait()

	if logicalErr != nil && !errors.Is(logicalErr, context.Canceled) {
		fmt.Fprintf(os.Stderr, "logical: %v\n", logicalErr)
	}
	if notifyErr != nil && !errors.Is(notifyErr, context.Canceled) {
		fmt.Fprintf(os.Stderr, "notify:  %v\n", notifyErr)
	}

	// Re-name the workloads so the table reads correctly.
	logicalRes.Summary.Workload = "logical"
	notifyRes.Summary.Workload = fmt.Sprintf("notify (missed=%d)", notifyRes.MissedWindows)

	fmt.Println()
	fmt.Println(benchstats.FormatTable([]benchstats.Summary{
		logicalRes.Summary,
		notifyRes.Summary,
	}))

	if *reconnect {
		fmt.Println("reconnect:")
		if logicalRes.ReplayedAfterGap {
			fmt.Printf("  logical replayed %d events from WAL after the gap (no loss).\n",
				logicalRes.EventsAfterReconnect)
		} else {
			fmt.Println("  logical did not record a reconnect (run too short or ctx canceled)")
		}
		if notifyRes.MissedWindows > 0 {
			fmt.Printf("  notify missed %d reconnect windows; events during the gap were lost.\n",
				notifyRes.MissedWindows)
		}
	}

	if err := writeJSON(*output, driverRes, logicalRes, notifyRes, *reconnect); err != nil {
		return fmt.Errorf("write results: %w", err)
	}
	fmt.Printf("wrote %s\n", *output)

	if *cleanupSlot {
		fmt.Printf("dropping slot %q\n", *slot)
		if err := invlogical.DropSlot(context.Background(), rwURL, *slot); err != nil {
			fmt.Fprintf(os.Stderr, "drop slot: %v (drop manually: SELECT pg_drop_replication_slot('%s'))\n",
				err, *slot)
		}
	} else {
		fmt.Printf("note: slot %q is left in place. drop it manually when done:\n", *slot)
		fmt.Printf("      SELECT pg_drop_replication_slot('%s');\n", *slot)
	}

	fmt.Println()
	fmt.Println("inv-bakeoff complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}

func writeJSON(
	path string,
	d invdriver.Result,
	l invlogical.Result,
	n invnotify.Result,
	reconnect bool,
) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Timestamp string             `json:"timestamp"`
		Reconnect bool               `json:"reconnect"`
		Driver    invdriver.Result   `json:"driver"`
		Logical   invlogical.Result  `json:"logical"`
		Notify    invnotify.Result   `json:"notify"`
	}{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Reconnect: reconnect,
		Driver:    d,
		Logical:   l,
		Notify:    n,
	})
}
