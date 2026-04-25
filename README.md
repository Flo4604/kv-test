# kv-spike

Throwaway harnesses to measure whether **PlanetScale Postgres** is fast
enough to back a KV service Unkey is considering. Three independent
benchmarks:

1. **`bench`** — point-read / point-write / batch-tx latency against a
   real PlanetScale Postgres instance.
2. **`inv-logical` + `inv-notify` + `inv-driver`** — write-to-cache-drop
   lag bakeoff between Postgres logical replication and `LISTEN/NOTIFY`,
   with a reconnect test that demonstrates the auto-backfill property
   of replication slots.
3. **`disktier`** — embedded KV bakeoff (Pebble vs Badger vs BoltDB)
   for the on-node cache layer that absorbs DB cold-starts.

This repo is intended to be run **inside the same datacenter as the
Postgres primary** so the numbers reflect realistic in-region latency.
Running it from a laptop on a different continent will only measure the
network round trip.

## Prereqs

- A PlanetScale Postgres branch with `kv_entries` from `schema.sql`
  applied.
- `DATABASE_URL` (and optionally `DATABASE_URL_RW` / `DATABASE_URL_RO`)
  set to that branch's connection strings.
- For `inv-logical`: a publication on the primary. Only needs to be
  created once:
  ```sh
  psql "$DATABASE_URL_RW" -c "CREATE PUBLICATION kv_invalidate_pub FOR TABLE kv_entries"
  ```

## Quick start (local)

```sh
go run ./cmd/bench seed
go run ./cmd/bench bench -duration=15s
```

The first command populates the corpus
(10 workspaces × 10 namespaces × 100k keys × 1 KB values).
The second runs the workload matrix and writes `bench_results.json`.

### bench flags

- `-workloads=Get,GetHot,GetMany_100,Put,Batch_5,Mixed_95_5` — filter
  which workloads to run. Empty means "all". Useful when drilling
  into one result (`-workloads=GetHot`).
- `-hot-keys=100` — `GetHot` reads are randomly drawn from the first
  N keys of `(ws_000, ns_000)` so the PG plan cache and buffer pool
  stay hot. Contrast with `Get`, which samples across all 10M seeded
  keys, mostly cold.
- `-loop` — repeat the workload matrix forever; `-loop-interval=30s`
  controls the sleep between runs.
- After one run (or when `-loop` is interrupted) the process blocks
  on SIGTERM instead of exiting, so container auto-restart does not
  churn. Send SIGTERM / SIGINT to exit.

## Run on Unkey Deploy (or any container platform)

```sh
docker build -t kv-spike .
```

The image bundles six binaries under `/usr/local/bin/`. Default `CMD`
runs `bench bench`. Override to run a different harness.

### Easiest path: one binary for the whole spike

`run-all` is the default `CMD`. Deploy with `DATABASE_URL_RW` set and
it runs `bench` → `inv-bakeoff`, then blocks on SIGTERM. **disktier
is skipped by default** because container ephemeral storage isn't
representative; run disktier locally for honest disk numbers (see
below).

```sh
docker run --rm -e DATABASE_URL_RW=... kv-spike
```

Tunables (all forwarded to the right child binary):

```sh
docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/run-all \
  -bench-duration=20s \
  -driver-duration=120s \
  -inv-reconnect

docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/run-all \
  -only=inv-bakeoff
```

### Just the invalidation bakeoff

```sh
docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/inv-bakeoff
```

`inv-bakeoff` runs the full bakeoff in one process: starts the
logical-replication subscriber and the LISTEN/NOTIFY subscriber, waits
for them to attach, runs the driver for 60s @ 1k/s, drains the
subscribers, prints both lag tables, and stays alive afterwards.
No coordination, no three deploys.

For the reconnect test:
```sh
docker run --rm -e DATABASE_URL_RW=... kv-spike \
  /usr/local/bin/inv-bakeoff -reconnect -driver-duration=120s
```
Both subscribers will cut after 30s, sleep 30s, and reconnect; the
output reports whether each one replayed its missed events.

### Advanced: standalone binaries

```sh
# disk tier bakeoff (no DB needed)
docker run --rm kv-spike /usr/local/bin/disktier -keys=100000 -duration=20s

# logical replication subscriber
docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/inv-logical -duration=120s

# LISTEN/NOTIFY subscriber
docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/inv-notify -duration=120s

# write driver (run in parallel with the subscribers)
docker run --rm -e DATABASE_URL_RW=... kv-spike /usr/local/bin/inv-driver -rate=1000 -duration=60s
```

Pass the database URL via env var (`DATABASE_URL` / `DATABASE_URL_RW` /
`DATABASE_URL_RO`) so it doesn't end up in your shell history.

## Targets

These are what we want to learn from the spike:

| Bench           | Target                                          |
|-----------------|-------------------------------------------------|
| `bench`         | p99 point-read **in-region** ≤ 5 ms             |
| `inv-logical`   | p99 commit-to-drop lag < 2 s at 1k writes/s     |
| `inv-notify`    | same, plus document the missed-window count     |
| `disktier`      | pick the engine with best p99 + reasonable size |

Cross-region numbers are documented but not gated.

## Reconnect test (invalidation)

```sh
# terminal 1 (writes)
docker run --rm -e DATABASE_URL_RW=... kv-spike \
  /usr/local/bin/inv-driver -rate=1000 -duration=120s

# terminal 2 (logical: replays via slot, no loss)
docker run --rm -e DATABASE_URL_RW=... kv-spike \
  /usr/local/bin/inv-logical -duration=120s -sleep-after=30s -sleep-duration=30s

# terminal 3 (notify: misses events during the gap)
docker run --rm -e DATABASE_URL_RW=... kv-spike \
  /usr/local/bin/inv-notify -duration=120s -sleep-after=30s -sleep-duration=30s
```

## Cleanup after the logical run

A replication slot retains WAL on the primary forever if abandoned.
Drop it when you're done:

```sh
psql "$DATABASE_URL_RW" -c "SELECT pg_drop_replication_slot('kv_invalidation_spike')"
psql "$DATABASE_URL_RW" -c "DROP PUBLICATION IF EXISTS kv_invalidate_pub"
```
