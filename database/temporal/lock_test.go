package temporal_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database"

	temporal "github.com/foomo/migrate/database/temporal"
)

func TestLockContention(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d1, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default", LockWorkflowID: "lk1", LockTTL: 30 * time.Second})
	d2, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default", LockWorkflowID: "lk1", LockTTL: 30 * time.Second})

	if err := d1.Lock(); err != nil {
		t.Fatalf("d1.Lock: %v", err)
	}
	if err := d2.Lock(); !errors.Is(err, database.ErrLocked) {
		t.Fatalf("d2.Lock = %v, want ErrLocked", err)
	}
	if err := d1.Unlock(); err != nil {
		t.Fatalf("d1.Unlock: %v", err)
	}
	// After unlock, d2 can acquire.
	if err := d2.Lock(); err != nil {
		t.Fatalf("d2.Lock after unlock: %v", err)
	}
	_ = d2.Unlock()
}

func TestUnlockNotLocked(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)

	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default", LockWorkflowID: "lk2"})
	if err := d.Unlock(); !errors.Is(err, database.ErrNotLocked) {
		t.Fatalf("Unlock = %v, want ErrNotLocked", err)
	}
}
