// Package dbconfig resolves Postgres connection URLs from flags or the
// environment and returns configured pgx pools.
//
// Three URLs are recognized:
//
//   - DATABASE_URL     plain URL used by single-pool harnesses
//     (bench seed, disk-tier bakeoff, invalidation driver)
//   - DATABASE_URL_RW  primary, used for writes and strong reads
//   - DATABASE_URL_RO  regional RO replica, used for cache-miss reads
//
// The bench harness treats DATABASE_URL as a fallback if the split URLs
// are not set.
package dbconfig

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Flags describes URL flags registered on a *flag.FlagSet. Call
// RegisterFlags before flag.Parse, then call RW/RO to pick the final
// values from flags/env.
type Flags struct {
	DatabaseURL   string
	DatabaseURLRW string
	DatabaseURLRO string
}

// RegisterFlags registers -database-url, -database-url-rw,
// -database-url-ro on the given flag set. Values default to the
// matching environment variables so either source works.
func RegisterFlags(fs *flag.FlagSet) *Flags {
	f := &Flags{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		DatabaseURLRW: os.Getenv("DATABASE_URL_RW"),
		DatabaseURLRO: os.Getenv("DATABASE_URL_RO"),
	}
	fs.StringVar(&f.DatabaseURL, "database-url", f.DatabaseURL, "Postgres URL; used when RW/RO are not specified")
	fs.StringVar(&f.DatabaseURLRW, "database-url-rw", f.DatabaseURLRW, "Postgres RW URL (PlanetScale primary)")
	fs.StringVar(&f.DatabaseURLRO, "database-url-ro", f.DatabaseURLRO, "Postgres RO URL (PlanetScale replica)")
	return f
}

// RW returns the RW URL, falling back to DatabaseURL.
func (f *Flags) RW() (string, error) {
	if f.DatabaseURLRW != "" {
		return f.DatabaseURLRW, nil
	}
	if f.DatabaseURL != "" {
		return f.DatabaseURL, nil
	}
	return "", errors.New("no RW database URL: set -database-url-rw or DATABASE_URL_RW (or -database-url/DATABASE_URL)")
}

// RO returns the RO URL, falling back to DatabaseURL (then RW).
func (f *Flags) RO() (string, error) {
	if f.DatabaseURLRO != "" {
		return f.DatabaseURLRO, nil
	}
	if f.DatabaseURL != "" {
		return f.DatabaseURL, nil
	}
	if f.DatabaseURLRW != "" {
		return f.DatabaseURLRW, nil
	}
	return "", errors.New("no RO database URL: set -database-url-ro or DATABASE_URL_RO (or -database-url/DATABASE_URL)")
}

// PoolOptions tunes a pgxpool.Pool. Zero values mean "use pgx default".
type PoolOptions struct {
	MinConns          int32
	MaxConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// Open parses the given URL and opens a pool with the given options.
// The caller owns the returned pool and must call Close.
func Open(ctx context.Context, url string, opts PoolOptions) (*pgxpool.Pool, error) {
	if url == "" {
		return nil, errors.New("dbconfig: empty URL")
	}
	cfg, err := pgxpool.ParseConfig(sanitizeURL(url))
	if err != nil {
		return nil, fmt.Errorf("dbconfig: parse URL: %w", err)
	}
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.MaxConnLifetime > 0 {
		cfg.MaxConnLifetime = opts.MaxConnLifetime
	}
	if opts.MaxConnIdleTime > 0 {
		cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	}
	if opts.HealthCheckPeriod > 0 {
		cfg.HealthCheckPeriod = opts.HealthCheckPeriod
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dbconfig: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dbconfig: ping: %w", err)
	}
	return pool, nil
}

// sanitizeURL percent-encodes characters that Go's net/url rejects in
// userinfo but that real-world Postgres providers use. PlanetScale
// Postgres usernames are formatted "<base>|<branch>" and the literal
// '|' is not legal in URI userinfo per RFC 3986, so net/url refuses
// to parse them. We rewrite only the userinfo segment.
func sanitizeURL(raw string) string {
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		return raw
	}
	afterScheme := schemeEnd + 3
	authorityEnd := len(raw)
	for i, c := range raw[afterScheme:] {
		if c == '/' || c == '?' || c == '#' {
			authorityEnd = afterScheme + i
			break
		}
	}
	authority := raw[afterScheme:authorityEnd]
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return raw
	}
	userinfo := authority[:at]
	if !strings.ContainsAny(userinfo, "|") {
		return raw
	}
	userinfo = strings.ReplaceAll(userinfo, "|", "%7C")
	return raw[:afterScheme] + userinfo + authority[at:] + raw[authorityEnd:]
}
