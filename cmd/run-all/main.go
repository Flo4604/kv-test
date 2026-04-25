// Command run-all orchestrates the full kv-spike on a single container.
//
// It runs disktier, then bench, then inv-bakeoff in sequence, with each
// child binary streaming directly to this process's stdout. After the
// last step, the orchestrator stays alive so Unkey Deploy auto-restart
// doesn't churn the container.
//
// Each child is spawned with KV_SPIKE_RUN_ALL=1 in its environment;
// children check that variable and skip their own keep-alive when set,
// so they exit cleanly after producing results and run-all proceeds to
// the next step immediately.
//
// Usage:
//
//	run-all                        # default args for every step
//	run-all -skip=disktier         # skip a step (comma-separated)
//	run-all -only=bench            # run only one step (overrides -skip)
//	run-all -bench-duration=20s    # forwarded to bench bench
//	run-all -driver-duration=120s  # forwarded to inv-bakeoff
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const binDir = "/usr/local/bin"

type step struct {
	name string
	bin  string
	args []string
}

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "run-all: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("run-all", flag.ExitOnError)

	skip := fs.String("skip", "", "comma-separated steps to skip (disktier, bench, inv-bakeoff)")
	only := fs.String("only", "", "comma-separated steps to run (overrides -skip)")

	// Per-step knobs the user typically wants to tune at deploy time.
	// disktier defaults: small corpus + short windows. The disk on
	// Unkey Deploy is not representative; for honest numbers run
	// disktier locally on a real NVMe. These defaults exist so
	// run-all completes in a reasonable time when included.
	disktierKeys := fs.Int("disktier-keys", 20_000, "disktier: number of distinct keys")
	disktierDuration := fs.Duration("disktier-duration", 8*time.Second, "disktier: per-workload duration")

	benchDuration := fs.Duration("bench-duration", 10*time.Second, "bench: per-workload duration")
	benchMaxConns := fs.Int("bench-max-conns", 20, "bench: max pool size")
	benchConcurrencies := fs.String("bench-concurrencies", "1,2,5,10", "bench: comma-separated concurrency levels")
	benchWorkloads := fs.String("bench-workloads", "", "bench: comma-separated workloads (empty=all)")

	invDriverDuration := fs.Duration("driver-duration", 60*time.Second, "inv-bakeoff: how long the writer runs")
	invDriverRate := fs.Int("driver-rate", 1000, "inv-bakeoff: writes per second")
	invReconnect := fs.Bool("inv-reconnect", false, "inv-bakeoff: run the reconnect test")
	invCleanupSlot := fs.Bool("inv-cleanup-slot", true, "inv-bakeoff: drop the replication slot after the run")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	steps := []step{
		{
			name: "disktier",
			bin:  binDir + "/disktier",
			args: []string{
				fmt.Sprintf("-keys=%d", *disktierKeys),
				fmt.Sprintf("-duration=%s", disktierDuration.String()),
			},
		},
		{
			name: "bench",
			bin:  binDir + "/bench",
			args: append([]string{
				"bench",
				fmt.Sprintf("-duration=%s", benchDuration.String()),
				fmt.Sprintf("-max-conns=%d", *benchMaxConns),
				fmt.Sprintf("-concurrencies=%s", *benchConcurrencies),
			}, optWorkloadsFlag(*benchWorkloads)...),
		},
		{
			name: "inv-bakeoff",
			bin:  binDir + "/inv-bakeoff",
			args: append([]string{
				fmt.Sprintf("-driver-duration=%s", invDriverDuration.String()),
				fmt.Sprintf("-driver-rate=%d", *invDriverRate),
				fmt.Sprintf("-cleanup-slot=%t", *invCleanupSlot),
			}, optReconnectFlag(*invReconnect)...),
		},
	}

	steps = filter(steps, *only, *skip)
	if len(steps) == 0 {
		return errors.New("no steps to run after applying -only/-skip")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	for i, s := range steps {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("\n========== [%d/%d] %s ==========\n", i+1, len(steps), s.name)
		if err := runStep(ctx, s); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			fmt.Fprintf(os.Stderr, "step %s failed: %v (continuing)\n", s.name, err)
		}
	}

	fmt.Println()
	fmt.Println("=================================================")
	fmt.Println("run-all complete; keeping container alive. send SIGTERM to exit.")
	fmt.Println("=================================================")
	<-ctx.Done()
	return nil
}

func runStep(ctx context.Context, s step) error {
	fmt.Printf("> %s %s\n", s.bin, strings.Join(s.args, " "))
	cmd := exec.CommandContext(ctx, s.bin, s.args...)
	cmd.Env = append(os.Environ(), "KV_SPIKE_RUN_ALL=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func filter(all []step, only, skip string) []step {
	if only != "" {
		want := commaSet(only)
		out := make([]step, 0, len(want))
		for _, s := range all {
			if _, ok := want[s.name]; ok {
				out = append(out, s)
			}
		}
		return out
	}
	if skip != "" {
		drop := commaSet(skip)
		out := make([]step, 0, len(all))
		for _, s := range all {
			if _, ok := drop[s.name]; ok {
				continue
			}
			out = append(out, s)
		}
		return out
	}
	return all
}

func commaSet(csv string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, name := range strings.Split(csv, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

func optWorkloadsFlag(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return []string{fmt.Sprintf("-workloads=%s", v)}
}

func optReconnectFlag(b bool) []string {
	if !b {
		return nil
	}
	return []string{"-reconnect"}
}
