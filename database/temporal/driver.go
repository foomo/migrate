package temporal

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gotime "github.com/foomo/go/time"
	"github.com/golang-migrate/migrate/v4/database"
	"go.temporal.io/sdk/client"
)

func init() {
	database.Register("temporal", &Driver{})
}

const (
	defaultNamespace      = "default"
	defaultLockTTL        = 5 * time.Minute
	defaultLockWorkflowID = "golang_migrate_lock"
	defaultTaskQueue      = "golang_migrate"

	versionKey = "schema_migrations_version"
	dirtyKey   = "schema_migrations_dirty"
)

// Config controls a Driver instance. All fields are optional with defaults.
type Config struct {
	// Namespace is the target namespace; version state lives in its Data map.
	Namespace string
	// LockTTL is the lock workflow's execution timeout and the orphan-lock
	// recovery window. It MUST exceed the duration of the longest migration:
	// if the lock workflow times out mid-run another process can acquire the
	// lock and run concurrently. Lower it only when all migrations are
	// known-fast and a shorter orphan-lock recovery window is desired.
	// Default: 5m.
	LockTTL time.Duration
	// LockWorkflowID is the fixed workflow ID used as the migration mutex.
	LockWorkflowID string
	// TaskQueue is the lock workflow's task queue (no worker consumes it).
	TaskQueue string
	// TLS enables TLS in the client connection (Temporal Cloud).
	TLS bool
	// APIKey is an API key credential (Temporal Cloud), if set.
	APIKey string
}

// Driver implements database.Driver backed by temporal.io.
type Driver struct {
	client client.Client
	config Config
	owns   bool // true when Open() dialed the client (so Close() closes it).
	locked atomic.Bool
}

// Open parses the URL, dials a client, and returns a connected Driver.
func (d *Driver) Open(rawURL string) (database.Driver, error) {
	hostPort, cfg, err := parseURL(rawURL)
	if err != nil {
		return nil, err
	}

	opts := client.Options{HostPort: hostPort, Namespace: cfg.Namespace}
	if cfg.TLS {
		opts.ConnectionOptions.TLS = &tls.Config{}
	}

	if cfg.APIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(cfg.APIKey)
	}

	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("temporal: dial: %w", err)
	}

	return &Driver{client: c, config: cfg, owns: true}, nil
}

// WithInstance wraps an already-connected client. Close() does not touch it.
func WithInstance(c client.Client, cfg *Config) (database.Driver, error) {
	if c == nil {
		return nil, errors.New("temporal: nil client")
	}

	return &Driver{client: c, config: normalizeConfig(cfg), owns: false}, nil
}

// Close closes the client only if Open dialed it.
func (d *Driver) Close() error {
	if d.client != nil && d.owns {
		d.client.Close()
	}

	d.client = nil

	return nil
}

func parseURL(rawURL string) (string, Config, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", Config{}, fmt.Errorf("temporal: invalid url: %w", err)
	}

	if u.Scheme != "temporal" {
		return "", Config{}, fmt.Errorf("temporal: expected scheme \"temporal\", got %q", u.Scheme)
	}

	cfg := normalizeConfig(nil)
	if ns := strings.TrimPrefix(u.Path, "/"); ns != "" {
		cfg.Namespace = ns
	}

	q := u.Query()
	if v := q.Get("lock_ttl"); v != "" {
		ttl, err := gotime.ParseDuration(v)
		if err != nil {
			return "", Config{}, fmt.Errorf("temporal: invalid lock_ttl %q: %w", v, err)
		}

		cfg.LockTTL = ttl
	}

	if v := q.Get("lock_workflow_id"); v != "" {
		cfg.LockWorkflowID = v
	}

	if v := q.Get("task_queue"); v != "" {
		cfg.TaskQueue = v
	}

	if v := q.Get("api_key"); v != "" {
		cfg.APIKey = v
	}

	if v := q.Get("tls"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return "", Config{}, fmt.Errorf("temporal: invalid tls %q: %w", v, err)
		}

		cfg.TLS = b
	}

	return u.Host, cfg, nil
}

func normalizeConfig(in *Config) Config {
	c := Config{
		Namespace:      defaultNamespace,
		LockTTL:        defaultLockTTL,
		LockWorkflowID: defaultLockWorkflowID,
		TaskQueue:      defaultTaskQueue,
	}
	if in == nil {
		return c
	}

	if in.Namespace != "" {
		c.Namespace = in.Namespace
	}

	if in.LockTTL > 0 {
		c.LockTTL = in.LockTTL
	}

	if in.LockWorkflowID != "" {
		c.LockWorkflowID = in.LockWorkflowID
	}

	if in.TaskQueue != "" {
		c.TaskQueue = in.TaskQueue
	}

	c.TLS = in.TLS
	c.APIKey = in.APIKey

	return c
}

// ensure interface compliance
var _ database.Driver = (*Driver)(nil)
