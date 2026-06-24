package nats

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() {
	database.Register("nats", &Driver{})
}

const (
	defaultBucket   = "schema_migrations"
	defaultLockTTL  = 15 * time.Second
	defaultReplicas = 1

	lockKey    = "lock"
	versionKey = "version"
)

// Config controls a Driver instance. All fields are optional and have defaults.
type Config struct {
	// Bucket is the JetStream KV bucket used for migration state.
	Bucket string
	// LockTTL is the TTL applied to the lock entry; orphaned locks auto-recover.
	LockTTL time.Duration
	// Replicas is the replica count for the state bucket on first creation.
	Replicas int
	// Domain is the JetStream domain to scope API requests to (optional).
	Domain string
	// Owner is an identifier embedded in the lock entry (host, pod, ...).
	// If empty, a UUID-ish value is generated.
	Owner string
}

// Driver implements database.Driver backed by NATS JetStream.
type Driver struct {
	conn    *natsgo.Conn
	js      jetstream.JetStream
	kv      jetstream.KeyValue
	config  Config
	owns    bool // true when Open() dialed the connection (so Close() drains it).
	locked  atomic.Bool
	lockRev uint64
}

// Open parses the URL and returns a connected Driver. Migrate calls Open once
// per instance via database.Register.
func (d *Driver) Open(rawURL string) (database.Driver, error) {
	purl, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("nats: invalid url: %w", err)
	}
	if purl.Scheme != "nats" {
		return nil, fmt.Errorf("nats: expected scheme \"nats\", got %q", purl.Scheme)
	}

	cfg := Config{
		Bucket:   defaultBucket,
		LockTTL:  defaultLockTTL,
		Replicas: defaultReplicas,
	}
	q := purl.Query()
	if v := q.Get("bucket"); v != "" {
		cfg.Bucket = v
	}
	if v := q.Get("lock_ttl"); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("nats: invalid lock_ttl %q: %w", v, err)
		}
		cfg.LockTTL = ttl
	}
	if v := q.Get("replicas"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("nats: invalid replicas %q", v)
		}
		cfg.Replicas = n
	}
	if v := q.Get("domain"); v != "" {
		cfg.Domain = v
	}
	if v := q.Get("owner"); v != "" {
		cfg.Owner = v
	}

	natsURL := stripQuery(purl)
	opts := []natsgo.Option{natsgo.Name("golang-migrate")}
	if purl.User != nil {
		if pw, ok := purl.User.Password(); ok {
			opts = append(opts, natsgo.UserInfo(purl.User.Username(), pw))
		}
	}
	conn, err := natsgo.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	out, err := newWithConn(conn, &cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	out.owns = true
	return out, nil
}

// WithInstance returns a Driver wrapping an already-connected nats.Conn. Use
// this from service startup code that already established a connection via
// keelnats.Connect or similar.
func WithInstance(conn *natsgo.Conn, cfg *Config) (database.Driver, error) {
	if conn == nil {
		return nil, errors.New("nats: nil connection")
	}
	c := normalizeConfig(cfg)
	d, err := newWithConn(conn, &c)
	if err != nil {
		return nil, err
	}
	d.owns = false
	return d, nil
}

func newWithConn(conn *natsgo.Conn, cfg *Config) (*Driver, error) {
	var (
		js  jetstream.JetStream
		err error
	)
	if cfg.Domain != "" {
		js, err = jetstream.NewWithDomain(conn, cfg.Domain)
	} else {
		js, err = jetstream.New(conn)
	}
	if err != nil {
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kv, err := ensureStateBucket(ctx, js, cfg)
	if err != nil {
		return nil, err
	}

	owner := cfg.Owner
	if owner == "" {
		owner = generateOwnerID()
	}
	cfg.Owner = owner

	return &Driver{
		conn:   conn,
		js:     js,
		kv:     kv,
		config: *cfg,
	}, nil
}

func ensureStateBucket(ctx context.Context, js jetstream.JetStream, cfg *Config) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, cfg.Bucket)
	if err == nil {
		return kv, nil
	}
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("nats: kv lookup %q: %w", cfg.Bucket, err)
	}
	created, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      cfg.Bucket,
		Description: "golang-migrate state for NATS JetStream driver",
		Replicas:    cfg.Replicas,
		TTL:         cfg.LockTTL * 8,
	})
	if err != nil {
		return nil, fmt.Errorf("nats: create kv bucket %q: %w", cfg.Bucket, err)
	}
	return created, nil
}

func normalizeConfig(in *Config) Config {
	c := Config{Bucket: defaultBucket, LockTTL: defaultLockTTL, Replicas: defaultReplicas}
	if in == nil {
		return c
	}
	if in.Bucket != "" {
		c.Bucket = in.Bucket
	}
	if in.LockTTL > 0 {
		c.LockTTL = in.LockTTL
	}
	if in.Replicas > 0 {
		c.Replicas = in.Replicas
	}
	c.Domain = in.Domain
	c.Owner = in.Owner
	return c
}

// Close releases driver resources. If the connection was dialed by Open it is
// drained here; connections handed in via WithInstance are not touched.
func (d *Driver) Close() error {
	if d.conn != nil && d.owns {
		if err := d.conn.Drain(); err != nil {
			return fmt.Errorf("nats: drain: %w", err)
		}
	}
	d.conn = nil
	d.js = nil
	d.kv = nil
	return nil
}

func stripQuery(u *url.URL) string {
	c := *u
	c.RawQuery = ""
	c.User = nil
	return c.String()
}

func generateOwnerID() string {
	return fmt.Sprintf("migrate-%d", time.Now().UnixNano())
}
