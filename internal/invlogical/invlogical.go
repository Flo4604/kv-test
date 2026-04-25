// Package invlogical is the logical-replication subscriber for the
// invalidation bakeoff.
package invlogical

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/unkeyed/kv-spike/internal/benchstats"
)

const outputPlugin = "pgoutput"

// Config controls a logical-subscriber run.
type Config struct {
	URL           string
	Slot          string
	Publication   string
	Duration      time.Duration
	SleepAfter    time.Duration // 0 disables the reconnect test
	SleepDuration time.Duration
}

// Result reports the subscriber's collected lag plus reconnect-test
// observations.
type Result struct {
	Summary              benchstats.Summary
	ReplayedAfterGap     bool
	EventsAfterReconnect int
}

// EnsureSlot creates the logical replication slot if it doesn't exist
// (idempotent). Useful to call once before launching the run so the
// slot is in place before the writer starts.
func EnsureSlot(ctx context.Context, url, slot string) error {
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

// DropSlot drops the logical replication slot. Caller is responsible
// for serialization. Best-effort; errors are returned for logging.
func DropSlot(ctx context.Context, url, slot string) error {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slot)).ReadAll()
	return err
}

// Run blocks until ctx is canceled or the configured duration elapses.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := EnsureSlot(ctx, cfg.URL, cfg.Slot); err != nil {
		return Result{}, err
	}

	rec := benchstats.NewRecorder(1024)
	deadline := time.Now().Add(cfg.Duration)

	replayedAfterGap := false
	eventsAfterReconnect := 0

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

		countBefore := rec.Count()
		err := streamOnce(sessionCtx, cfg.URL, cfg.Slot, cfg.Publication, rec)
		sessionCancel()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}

		if cfg.SleepAfter > 0 && time.Now().Before(deadline) {
			sleep := cfg.SleepDuration
			if until := time.Until(deadline); until < sleep {
				sleep = until
			}
			select {
			case <-ctx.Done():
			case <-time.After(sleep):
			}
			cfg.SleepAfter = 0
			countAfter := rec.Count()
			eventsAfterReconnect = countAfter - countBefore
			replayedAfterGap = true
		} else {
			break
		}
	}

	summary := rec.Summarize("logical_lag", 1, cfg.Duration)
	return Result{
		Summary:              summary,
		ReplayedAfterGap:     replayedAfterGap,
		EventsAfterReconnect: eventsAfterReconnect,
	}, nil
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
