// Command inv-driver generates writes for the invalidation bakeoff.
// Thin wrapper around internal/invdriver.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/unkeyed/kv-spike/internal/dbconfig"
	"github.com/unkeyed/kv-spike/internal/invdriver"
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

	fmt.Printf("inv-driver: rate=%d/s duration=%s key-space=%d workers=%d\n",
		*rate, duration.String(), *keySpace, *workers)

	res, err := invdriver.Run(ctx, invdriver.Config{
		URL:        rwURL,
		Rate:       *rate,
		Duration:   *duration,
		KeySpace:   *keySpace,
		ValueBytes: *valueBytes,
		Workers:    *workers,
	})
	if err != nil {
		return err
	}
	fmt.Println(res.Format())

	fmt.Println("inv-driver run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}
