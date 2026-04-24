// Package keygen produces deterministic (workspace, namespace, key)
// tuples for the spike workloads. Deterministic keys mean seed runs
// and bench runs agree on what exists.
package keygen

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
)

// Layout describes the seeded corpus: W workspaces, N namespaces per
// workspace, K keys per (workspace, namespace). Values are the raw
// byte size of the value blob.
type Layout struct {
	Workspaces     int
	NamespacesEach int
	KeysEach       int
	ValueBytes     int
}

// Default matches the plan: 10 workspaces x 10 namespaces x 100k keys
// x 1KB values.
func Default() Layout {
	return Layout{
		Workspaces:     10,
		NamespacesEach: 10,
		KeysEach:       100_000,
		ValueBytes:     1024,
	}
}

// TotalKeys is the count of rows in the seeded table.
func (l Layout) TotalKeys() int {
	return l.Workspaces * l.NamespacesEach * l.KeysEach
}

// Workspace returns the workspace id for index w in [0, Workspaces).
func (l Layout) Workspace(w int) string {
	return fmt.Sprintf("ws_%03d", w)
}

// Namespace returns the namespace name for index n in [0, NamespacesEach).
func (l Layout) Namespace(n int) string {
	return fmt.Sprintf("ns_%03d", n)
}

// Key returns the key for index k in [0, KeysEach).
func (l Layout) Key(k int) string {
	return fmt.Sprintf("key_%07d", k)
}

// RandomTuple returns a uniformly random (workspace, namespace, key)
// from the seeded corpus using the supplied rng.
func (l Layout) RandomTuple(r *mrand.Rand) (string, string, string) {
	w := r.IntN(l.Workspaces)
	n := r.IntN(l.NamespacesEach)
	k := r.IntN(l.KeysEach)
	return l.Workspace(w), l.Namespace(n), l.Key(k)
}

// Value returns a deterministic pseudo-random value buffer of the
// configured size, seeded off a stable input so the seeder is
// reproducible.
func (l Layout) Value(seed uint64) []byte {
	out := make([]byte, l.ValueBytes)
	src := mrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
	r := mrand.New(src)
	for i := range out {
		out[i] = byte(r.UintN(256))
	}
	return out
}

// RandomValue fills buf with cryptographically random bytes. Used by
// the invalidation driver when determinism is not useful.
func RandomValue(buf []byte) {
	_, _ = rand.Read(buf)
}
