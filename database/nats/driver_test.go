package nats_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/foomo/migrate/database/nats"
	"github.com/golang-migrate/migrate/v4/database"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// Suppress unused imports in case a future refactor drops one.
var _ = io.EOF

func TestOpenRejectsBadScheme(t *testing.T) {
	d := &nats.Driver{}
	_, err := d.Open("http://localhost:4222")
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheme")
}

func TestOpenRejectsBadLockTTL(t *testing.T) {
	d := &nats.Driver{}
	_, err := d.Open("nats://localhost:4222/?lock_ttl=banana")
	require.Error(t, err)
	require.Contains(t, err.Error(), "lock_ttl")
}

func TestOpenRejectsBadReplicas(t *testing.T) {
	d := &nats.Driver{}
	_, err := d.Open("nats://localhost:4222/?replicas=0")
	require.Error(t, err)
	require.Contains(t, err.Error(), "replicas")
}

func TestLockUnlockRoundTrip(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	require.NoError(t, d.Lock())
	require.Equal(t, database.ErrLocked, d.Lock())
	require.NoError(t, d.Unlock())
	require.Equal(t, database.ErrNotLocked, d.Unlock())
}

func TestLockBlocksAcrossDrivers(t *testing.T) {
	endpoint := startJetStream(t)
	a := openDriver(t, endpoint)
	b := openDriver(t, endpoint)

	require.NoError(t, a.Lock())
	require.Equal(t, database.ErrLocked, b.Lock())
	require.NoError(t, a.Unlock())
	require.NoError(t, b.Lock())
	require.NoError(t, b.Unlock())
}

func TestVersionRoundTrip(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	v, dirty, err := d.Version()
	require.NoError(t, err)
	require.Equal(t, database.NilVersion, v)
	require.False(t, dirty)

	require.NoError(t, d.SetVersion(7, true))
	v, dirty, err = d.Version()
	require.NoError(t, err)
	require.Equal(t, 7, v)
	require.True(t, dirty)

	require.NoError(t, d.SetVersion(8, false))
	v, dirty, err = d.Version()
	require.NoError(t, err)
	require.Equal(t, 8, v)
	require.False(t, dirty)

	require.NoError(t, d.SetVersion(database.NilVersion, false))
	v, _, err = d.Version()
	require.NoError(t, err)
	require.Equal(t, database.NilVersion, v)
}

func TestRunCreatesStreamAndConsumer(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	migration := `[
		{"op":"create_stream","config":{"name":"ORDERS","subjects":["orders.>"]}},
		{"op":"create_consumer","stream":"ORDERS","config":{"durable_name":"worker","ack_policy":"explicit"}}
	]`
	require.NoError(t, d.Run(strings.NewReader(migration)))

	js := newJSForTest(t, endpoint)
	ctx := context.Background()

	stream, err := js.Stream(ctx, "ORDERS")
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"orders.>"}, info.Config.Subjects)

	_, err = js.Consumer(ctx, "ORDERS", "worker")
	require.NoError(t, err)
}

func TestRunUpdateAndDelete(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	require.NoError(t, d.Run(strings.NewReader(`[{"op":"create_stream","config":{"name":"X","subjects":["x.a"]}}]`)))
	require.NoError(t, d.Run(strings.NewReader(`[{"op":"update_stream","config":{"name":"X","subjects":["x.a","x.b"]}}]`)))

	js := newJSForTest(t, endpoint)
	info, err := mustStreamInfo(js, "X")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"x.a", "x.b"}, info.Config.Subjects)

	require.NoError(t, d.Run(strings.NewReader(`[{"op":"delete_stream","name":"X"}]`)))
	_, err = mustStreamInfo(js, "X")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

func TestRunKVOps(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	val := base64.StdEncoding.EncodeToString([]byte("hello"))
	mig := fmt.Sprintf(`[
		{"op":"create_kv","config":{"bucket":"settings"}},
		{"op":"kv_put","bucket":"settings","key":"greeting","value_b64":%q}
	]`, val)
	require.NoError(t, d.Run(strings.NewReader(mig)))

	js := newJSForTest(t, endpoint)
	kv, err := js.KeyValue(context.Background(), "settings")
	require.NoError(t, err)
	entry, err := kv.Get(context.Background(), "greeting")
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), entry.Value())

	require.NoError(t, d.Run(strings.NewReader(`[{"op":"kv_delete","bucket":"settings","key":"greeting"}]`)))
	_, err = kv.Get(context.Background(), "greeting")
	require.Error(t, err)
}

func TestRunEmptyIsNoOp(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)
	require.NoError(t, d.Run(strings.NewReader("   \n\t  ")))
	require.NoError(t, d.Run(strings.NewReader("")))
}

func TestRunRejectsUnknownOp(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)
	err := d.Run(strings.NewReader(`[{"op":"yolo"}]`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown op")
}

func TestDropWipesEverything(t *testing.T) {
	endpoint := startJetStream(t)
	d := openDriver(t, endpoint)

	require.NoError(t, d.Run(strings.NewReader(`[
		{"op":"create_stream","config":{"name":"A","subjects":["a"]}},
		{"op":"create_stream","config":{"name":"B","subjects":["b"]}},
		{"op":"create_kv","config":{"bucket":"data"}}
	]`)))
	require.NoError(t, d.SetVersion(3, false))
	require.NoError(t, d.Drop())

	js := newJSForTest(t, endpoint)
	ctx := context.Background()
	sl := js.StreamNames(ctx)
	streams, err := collectNames(sl.Name(), sl.Err)
	require.NoError(t, err)
	require.Empty(t, streams)
	kl := js.KeyValueStoreNames(ctx)
	buckets, err := collectNames(kl.Name(), kl.Error)
	require.NoError(t, err)
	require.Empty(t, buckets)
}

func TestWithInstanceUsesExistingConn(t *testing.T) {
	endpoint := startJetStream(t)
	conn, err := natsgo.Connect(strings.TrimPrefix(endpoint, "nats://"))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	d, err := nats.WithInstance(conn, &nats.Config{Bucket: "custom", LockTTL: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	require.NoError(t, d.SetVersion(42, false))
	v, _, err := d.Version()
	require.NoError(t, err)
	require.Equal(t, 42, v)

	// Caller-owned connection survives driver.Close().
	require.NoError(t, d.Close())
	require.True(t, conn.IsConnected())
}

// Ensure Op JSON round-trips through encoding/json without dropping fields.
func TestOpJSONRoundTrip(t *testing.T) {
	op := nats.Op{
		Kind:     "create_stream",
		Config:   json.RawMessage(`{"name":"X","subjects":["x.>"]}`),
		Stream:   "X",
		Bucket:   "b",
		Name:     "n",
		Key:      "k",
		ValueB64: base64.StdEncoding.EncodeToString([]byte("v")),
	}
	encoded, err := json.Marshal(op)
	require.NoError(t, err)
	var dec nats.Op
	require.NoError(t, json.Unmarshal(encoded, &dec))
	require.Equal(t, op.Kind, dec.Kind)
	require.Equal(t, op.Stream, dec.Stream)
}

// --- Integration tests below: spin up a JetStream container and exercise the
// driver end-to-end. ---

func startJetStream(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// https://hub.docker.com/_/nats/tags
	container, err := tcnats.Run(ctx, "nats:2.14.2-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	endpoint, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	return endpoint
}

func openDriver(t *testing.T, endpoint string) database.Driver {
	t.Helper()
	d := &nats.Driver{}
	out, err := d.Open(endpoint + "/?bucket=schema_migrations&lock_ttl=2s")
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close() })
	return out
}

// helpers

func newJSForTest(t *testing.T, endpoint string) jetstream.JetStream {
	t.Helper()
	conn, err := natsgo.Connect(strings.TrimPrefix(endpoint, "nats://"))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	return js
}

func mustStreamInfo(js jetstream.JetStream, name string) (*jetstream.StreamInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.Info(ctx)
}

func collectNames(ch <-chan string, errFn func() error) ([]string, error) {
	out := make([]string, 0)
	for name := range ch {
		out = append(out, name)
	}
	if err := errFn(); err != nil {
		return nil, err
	}
	return out, nil
}
