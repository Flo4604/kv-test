// Command inv-logical is the logical-replication subscriber for the
// invalidation bakeoff. Thin wrapper around internal/invlogical.
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

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/invlogical"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "inv-logical: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("inv-logical", flag.ExitOnError)
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL for the primary (replication access)")
	databaseURLRW := fs.String("database-url-rw", os.Getenv("DATABASE_URL_RW"), "Postgres URL for the primary (replication access)")
	slot := fs.String("slot", "kv_invalidation_spike", "replication slot name")
	publication := fs.String("publication", "kv_invalidate_pub", "publication name")
	duration := fs.Duration("duration", 60*time.Second, "total run time")
	sleepAfter := fs.Duration("sleep-after", 0, "if >0, close the replication connection after this long to simulate a cut")
	sleepDuration := fs.Duration("sleep-duration", 30*time.Second, "how long to stay disconnected before reconnecting")
	output := fs.String("output", "logical_lag.json", "JSON results path")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	url := *databaseURLRW
	if url == "" {
		url = *databaseURL
	}
	if url == "" {
		return errors.New("no database URL: set -database-url-rw or DATABASE_URL_RW")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("inv-logical: slot=%s publication=%s duration=%s sleep-after=%s\n",
		*slot, *publication, duration.String(), sleepAfter.String())

	res, err := invlogical.Run(ctx, invlogical.Config{
		URL:           url,
		Slot:          *slot,
		Publication:   *publication,
		Duration:      *duration,
		SleepAfter:    *sleepAfter,
		SleepDuration: *sleepDuration,
	})
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println(benchstats.FormatTable([]benchstats.Summary{res.Summary}))
	if res.ReplayedAfterGap {
		fmt.Printf("reconnect: replayed %d events from WAL after the simulated cut (no data loss).\n",
			res.EventsAfterReconnect)
	}

	if err := writeJSONLogical(*output, res); err != nil {
		return err
	}

	if os.Getenv("KV_SPIKE_RUN_ALL") == "" {
		fmt.Println("inv-logical run complete; keeping container alive. send SIGTERM to exit.")
		<-ctx.Done()
	}
	return nil
}

func writeJSONLogical(path string, r invlogical.Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Summary              benchstats.Summary `json:"summary"`
		ReplayedAfterGap     bool               `json:"replayed_after_gap"`
		EventsAfterReconnect int                `json:"events_after_reconnect"`
		BackfillProperty     string             `json:"backfill_property"`
	}{
		Summary:              r.Summary,
		ReplayedAfterGap:     r.ReplayedAfterGap,
		EventsAfterReconnect: r.EventsAfterReconnect,
		BackfillProperty:     "Logical replication slots retain WAL on the primary while a subscriber is disconnected. Reconnecting resumes from the last confirmed LSN: no events lost.",
	})
}
