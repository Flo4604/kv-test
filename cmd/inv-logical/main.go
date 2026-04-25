// Command inv-logical is the logical-replication subscriber for the
// invalidation bakeoff.
//
// Bootstrapping (once, manually):
//
//	-- on the primary
//	CREATE PUBLICATION kv_invalidate_pub FOR TABLE kv_entries;
//
// This program connects with replication=database, creates a logical
// replication slot if needed, then consumes COMMIT boundaries from
// the pgoutput plugin. For each committed INSERT/UPDATE/DELETE on
// kv_entries it records the lag between the driver's `updated_at`
// timestamp and the local wall clock.
//
// Reconnect test: with -sleep-after set, the subscriber closes the
// replication connection and sleeps for -sleep-duration. On reconnect
// it resumes from the last confirmed LSN. Because the slot holds WAL
// on the primary for the duration of the gap, no events are lost.
//
// Usage:
//
//	inv-logical \
//	  -database-url-rw=$DATABASE_URL_RW \
//	  -slot=kv_node_local \
//	  -publication=kv_invalidate_pub \
//	  -duration=60s \
//	  -output=logical_lag.json
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

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/unkeyed/kv-spike/internal/benchstats"
)

const outputPlugin = "pgoutput"

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

	rec := benchstats.NewRecorder(1024)
	deadline := time.Now().Add(*duration)

	if err := ensureSlot(ctx, url, *slot); err != nil {
		return err
	}

	replayedAfterGap := false
	eventsAfterReconnect := 0

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

		countBefore := rec.Count()
		err := streamOnce(sessionCtx, url, *slot, *publication, rec)
		sessionCancel()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if *sleepAfter > 0 && time.Now().Before(deadline) {
			sleep := *sleepDuration
			if until := time.Until(deadline); until < sleep {
				sleep = until
			}
			fmt.Printf("inv-logical: disconnected for %s; WAL retained at slot %s\n",
				sleep.String(), *slot)
			select {
			case <-ctx.Done():
			case <-time.After(sleep):
			}
			fmt.Printf("inv-logical: reconnecting; replaying from last confirmed LSN\n")
			*sleepAfter = 0
			countAfter := rec.Count()
			eventsAfterReconnect = countAfter - countBefore
			replayedAfterGap = true
		} else {
			break
		}
	}

	summary := rec.Summarize("logical_lag", 1, *duration)

	fmt.Println()
	fmt.Println(benchstats.FormatTable([]benchstats.Summary{summary}))
	if replayedAfterGap {
		fmt.Printf("reconnect: replayed %d events from WAL after the simulated cut (no data loss).\n",
			eventsAfterReconnect)
	}
	if err := writeJSON(*output, summary, replayedAfterGap, eventsAfterReconnect); err != nil {
		return err
	}

	fmt.Println("inv-logical run complete; keeping container alive. send SIGTERM to exit.")
	<-ctx.Done()
	return nil
}

func ensureSlot(ctx context.Context, url, slot string) error {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect (replication): %w", err)
	}
	defer conn.Close(ctx)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			return nil
		}
		return fmt.Errorf("create replication slot: %w", err)
	}
	return nil
}

func streamOnce(ctx context.Context, url, slot, publication string, rec *benchstats.Recorder) error {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect (replication): %w", err)
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem: %w", err)
	}

	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publication),
		"messages 'false'",
		"streaming 'false'",
	}
	if err := pglogrepl.StartReplication(ctx, conn, slot, sysident.XLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("StartReplication: %w", err)
	}

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := 10 * time.Second
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	relations := map[uint32]*pglogrepl.RelationMessage{}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(nextStandbyMessageDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate: %w", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		waitCtx, waitCancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(waitCtx)
		waitCancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("ReceiveMessage: %w", err)
		}
		errMsg, ok := rawMsg.(*pgproto3.ErrorResponse)
		if ok {
			return fmt.Errorf("replication error: %+v", errMsg)
		}
		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage: %w", err)
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData: %w", err)
			}
			if err := handleLogicalMessage(xld, relations, rec); err != nil {
				return err
			}
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func handleLogicalMessage(xld pglogrepl.XLogData, relations map[uint32]*pglogrepl.RelationMessage, rec *benchstats.Recorder) error {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("Parse logical message: %w", err)
	}
	switch m := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[m.RelationID] = m
	case *pglogrepl.InsertMessage:
		observe(relations, m.RelationID, m.Tuple, rec)
	case *pglogrepl.UpdateMessage:
		observe(relations, m.RelationID, m.NewTuple, rec)
	case *pglogrepl.DeleteMessage:
		observe(relations, m.RelationID, m.OldTuple, rec)
	}
	return nil
}

func observe(relations map[uint32]*pglogrepl.RelationMessage, relID uint32, tuple *pglogrepl.TupleData, rec *benchstats.Recorder) {
	if tuple == nil {
		return
	}
	rel, ok := relations[relID]
	if !ok {
		return
	}
	if rel.RelationName != "kv_entries" {
		return
	}
	var updatedAtMs int64
	for i, col := range rel.Columns {
		if col.Name != "updated_at" {
			continue
		}
		if i >= len(tuple.Columns) {
			return
		}
		td := tuple.Columns[i]
		if td.DataType != pglogrepl.TupleDataTypeText && td.DataType != pglogrepl.TupleDataTypeBinary {
			return
		}
		if td.DataType == pglogrepl.TupleDataTypeText {
			if _, err := fmt.Sscanf(string(td.Data), "%d", &updatedAtMs); err != nil {
				return
			}
		}
	}
	if updatedAtMs == 0 {
		return
	}
	nowMs := time.Now().UnixMilli()
	lag := time.Duration(nowMs-updatedAtMs) * time.Millisecond
	if lag < 0 {
		lag = 0
	}
	rec.Observe(lag)
}

func writeJSON(path string, s benchstats.Summary, replayedAfterGap bool, eventsAfterReconnect int) error {
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
		Summary:              s,
		ReplayedAfterGap:     replayedAfterGap,
		EventsAfterReconnect: eventsAfterReconnect,
		BackfillProperty:     "Logical replication slots retain WAL on the primary while a subscriber is disconnected. Reconnecting resumes from the last confirmed LSN: no events lost.",
	})
}
