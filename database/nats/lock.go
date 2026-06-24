package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/nats-io/nats.go/jetstream"
)

type lockPayload struct {
	Owner      string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// Lock acquires the cluster-wide migration lock by atomically creating a
// well-known key in the state KV bucket. Returns database.ErrLocked if another
// migration process already holds it.
func (d *Driver) Lock() error {
	if !d.locked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	payload, err := json.Marshal(lockPayload{
		Owner:      d.config.Owner,
		AcquiredAt: time.Now().UTC(),
	})
	if err != nil {
		d.locked.Store(false)
		return fmt.Errorf("nats: marshal lock payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rev, err := d.kv.Create(ctx, lockKey, payload)
	if err != nil {
		d.locked.Store(false)
		if isKeyExistsErr(err) {
			return database.ErrLocked
		}
		return fmt.Errorf("nats: acquire lock: %w", err)
	}
	d.lockRev = rev
	return nil
}

// Unlock releases the lock. Returns database.ErrNotLocked if the local mirror
// shows the driver doesn't hold it.
func (d *Driver) Unlock() error {
	if !d.locked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := d.kv.Purge(ctx, lockKey); err != nil {
		if isKeyNotFoundErr(err) {
			return nil
		}
		// restore local state so caller can retry
		d.locked.Store(true)
		return fmt.Errorf("nats: release lock: %w", err)
	}
	d.lockRev = 0
	return nil
}

func isKeyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	// nats-io occasionally surfaces this as a "wrong last sequence" API error
	// when the key already has revisions; treat as the same condition.
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
		return true
	}
	return false
}

func isKeyNotFoundErr(err error) bool {
	return err != nil && errors.Is(err, jetstream.ErrKeyNotFound)
}
