// Package workload describes the shared event shape between the
// invalidation driver and the two subscriber implementations. The
// driver writes a row, tags it with a timestamp in `updated_at`, and
// subscribers compute latency from (`updated_at`, `now()`).
package workload

import (
	"fmt"
	"strings"
)

// DropEvent is what a subscriber produces when it sees a write to
// kv_entries. UpdatedAtMs is read out of the row (or the NOTIFY
// payload) and lets the driver compute end-to-end lag.
type DropEvent struct {
	WorkspaceID string
	Namespace   string
	Key         string
	UpdatedAtMs int64
}

// EncodeNotifyPayload packs a (workspace, namespace, key) tuple into a
// NOTIFY payload. Keep this in sync with the trigger in schema.sql if
// you change the format.
//
// Format: "<workspace_id>|<namespace>|<key>". The notify subscriber
// does a follow-up SELECT to read updated_at; that read is out-of-band
// of the latency measurement but kept fast (indexed by PK).
func EncodeNotifyPayload(workspace, namespace, key string) string {
	return fmt.Sprintf("%s|%s|%s", workspace, namespace, key)
}

// DecodeNotifyPayload is the inverse of EncodeNotifyPayload.
func DecodeNotifyPayload(payload string) (workspace, namespace, key string, err error) {
	parts := strings.SplitN(payload, "|", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("bad notify payload: %q", payload)
	}
	return parts[0], parts[1], parts[2], nil
}

// Workspace and namespace identifiers the driver uses. Kept small so
// the harness is self-contained and doesn't depend on a seeded corpus.
const (
	Workspace = "ws_invalidation_bench"
	Namespace = "ns_invalidation_bench"
)
