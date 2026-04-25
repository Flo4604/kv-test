// Command inv-notify is the LISTEN/NOTIFY subscriber for the
// invalidation bakeoff. Thin wrapper around internal/invnotify.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/unkeyed/kv-spike/internal/benchstats"
	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/invnotify"
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

	fmt.Printf("inv-notify: listening for %s (sleep-after=%s sleep-duration=%s)\n",
		duration.String(), sleepAfter.String(), sleepDuration.String())

	res, err := invnotify.Run(ctx, invnotify.Config{
		URL:           rwURL,
		Duration:      *duration,
		SleepAfter:    *sleepAfter,
		SleepDuration: *sleepDuration,
	})
	if err != nil {
		return err
	}

	res.Summary.Workload = fmt.Sprintf("notify_lag (missed windows=%d)", res.MissedWindows)
	fmt.Println()
	fmt.Println(benchstats.FormatTable([]benchstats.Summary{res.Summary}))
	if res.MissedWindows > 0 {
		fmt.Println("NOTE: events produced during the disconnect window are permanently lost.")
		fmt.Println("      An anti-entropy sweep is required to make LISTEN/NOTIFY viable.")
	}

	if err := writeJSONNotify(*output, res); err != nil {
		return err
	}

	fmt.Println("inv-notify run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}

func writeJSONNotify(path string, r invnotify.Result) error {
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
		Summary:          r.Summary,
		MissedWindows:    r.MissedWindows,
		BackfillProperty: "LISTEN/NOTIFY has no server-side backlog. Events produced while the subscriber is disconnected are lost. An anti-entropy sweep is required.",
	})
}
