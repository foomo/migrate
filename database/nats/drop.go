package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Drop deletes every stream and every KV bucket reachable on the current
// JetStream connection — including the migration state bucket. Per the
// database.Driver contract this is destructive; the caller must Open() again
// to reuse the driver.
func (d *Driver) Drop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	streams, err := collectStreamNames(ctx, d.js)
	if err != nil {
		return fmt.Errorf("nats: list streams: %w", err)
	}
	buckets, err := collectKVNames(ctx, d.js)
	if err != nil {
		return fmt.Errorf("nats: list kv buckets: %w", err)
	}

	for _, name := range buckets {
		if err := d.js.DeleteKeyValue(ctx, name); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("nats: delete kv %q: %w", name, err)
		}
	}

	for _, name := range streams {
		if isKVBackingStream(name, buckets) {
			continue
		}
		if err := d.js.DeleteStream(ctx, name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("nats: delete stream %q: %w", name, err)
		}
	}

	d.kv = nil
	d.locked.Store(false)
	d.lockRev = 0
	return nil
}

func collectStreamNames(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	lister := js.StreamNames(ctx)
	out := make([]string, 0)
	for name := range lister.Name() {
		out = append(out, name)
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func collectKVNames(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	lister := js.KeyValueStoreNames(ctx)
	out := make([]string, 0)
	for name := range lister.Name() {
		out = append(out, name)
	}
	if err := lister.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// isKVBackingStream skips streams that JetStream owns as KV backing storage —
// DeleteKeyValue already removed them, and DeleteStream on the backing stream
// would error.
func isKVBackingStream(streamName string, kvBuckets []string) bool {
	const prefix = "KV_"
	if len(streamName) <= len(prefix) || streamName[:len(prefix)] != prefix {
		return false
	}
	bucket := streamName[len(prefix):]
	for _, b := range kvBuckets {
		if b == bucket {
			return true
		}
	}
	return false
}
