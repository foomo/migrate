package temporal_test

import (
	"testing"

	"github.com/golang-migrate/migrate/v4/database"

	temporal "github.com/foomo/migrate/database/temporal"
)

func TestVersionRoundTrip(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)

	d, err := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})
	if err != nil {
		t.Fatalf("WithInstance: %v", err)
	}

	// No version set yet.
	v, dirty, err := d.Version()
	if err != nil {
		t.Fatalf("Version: %v", err)
	}

	if v != database.NilVersion || dirty {
		t.Fatalf("initial version = (%d,%v), want (%d,false)", v, dirty, database.NilVersion)
	}

	// Set and read back.
	if err := d.SetVersion(7, true); err != nil {
		t.Fatalf("SetVersion: %v", err)
	}

	v, dirty, err = d.Version()
	if err != nil {
		t.Fatalf("Version: %v", err)
	}

	if v != 7 || !dirty {
		t.Fatalf("version = (%d,%v), want (7,true)", v, dirty)
	}

	// NilVersion clears.
	if err := d.SetVersion(database.NilVersion, false); err != nil {
		t.Fatalf("clear: %v", err)
	}

	v, _, err = d.Version()
	if err != nil {
		t.Fatalf("Version after clear: %v", err)
	}

	if v != database.NilVersion {
		t.Fatalf("after clear version = %d, want %d", v, database.NilVersion)
	}
}
