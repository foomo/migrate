package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
)

type versionPayload struct {
	Version int  `json:"version"`
	Dirty   bool `json:"dirty"`
}

// SetVersion persists version + dirty flag in the state bucket. -1 erases the
// entry, matching database.NilVersion.
func (d *Driver) SetVersion(version int, dirty bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if version == database.NilVersion {
		if err := d.kv.Purge(ctx, versionKey); err != nil && !isKeyNotFoundErr(err) {
			return fmt.Errorf("nats: clear version: %w", err)
		}
		return nil
	}

	payload, err := json.Marshal(versionPayload{Version: version, Dirty: dirty})
	if err != nil {
		return fmt.Errorf("nats: marshal version: %w", err)
	}
	if _, err := d.kv.Put(ctx, versionKey, payload); err != nil {
		return fmt.Errorf("nats: store version: %w", err)
	}
	return nil
}

// Version returns the current migration version and dirty flag. When no
// version has been stored yet it returns (database.NilVersion, false, nil).
func (d *Driver) Version() (int, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entry, err := d.kv.Get(ctx, versionKey)
	if err != nil {
		if isKeyNotFoundErr(err) {
			return database.NilVersion, false, nil
		}
		return 0, false, fmt.Errorf("nats: read version: %w", err)
	}
	var p versionPayload
	if err := json.Unmarshal(entry.Value(), &p); err != nil {
		return 0, false, fmt.Errorf("nats: decode version: %w", err)
	}
	return p.Version, p.Dirty, nil
}
