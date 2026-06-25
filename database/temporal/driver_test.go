package temporal_test

import (
	"testing"
	"time"

	"github.com/foomo/migrate/database/temporal"
)

func TestParseURL(t *testing.T) {
	hostPort, cfg, err := temporal.ParseURL(
		"temporal://localhost:7233/myns?lock_ttl=30s&lock_workflow_id=lk&task_queue=tq&tls=true&api_key=secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hostPort != "localhost:7233" {
		t.Errorf("hostPort = %q, want localhost:7233", hostPort)
	}

	if cfg.Namespace != "myns" {
		t.Errorf("Namespace = %q, want myns", cfg.Namespace)
	}

	if cfg.LockTTL != 30*time.Second {
		t.Errorf("LockTTL = %v, want 30s", cfg.LockTTL)
	}

	if cfg.LockWorkflowID != "lk" || cfg.TaskQueue != "tq" {
		t.Errorf("lock/taskqueue = %q/%q", cfg.LockWorkflowID, cfg.TaskQueue)
	}

	if !cfg.TLS || cfg.APIKey != "secret" {
		t.Errorf("tls/apikey = %v/%q", cfg.TLS, cfg.APIKey)
	}
}

func TestParseURLExtendedUnits(t *testing.T) {
	for _, tc := range []struct {
		ttl  string
		want time.Duration
	}{
		{"2d", 48 * time.Hour},
		{"1w", 168 * time.Hour},
	} {
		_, cfg, err := temporal.ParseURL("temporal://localhost:7233/myns?lock_ttl=" + tc.ttl)
		if err != nil {
			t.Fatalf("lock_ttl=%s: unexpected error: %v", tc.ttl, err)
		}
		if cfg.LockTTL != tc.want {
			t.Errorf("lock_ttl=%s: LockTTL = %v, want %v", tc.ttl, cfg.LockTTL, tc.want)
		}
	}
}

func TestParseURLRejectsScheme(t *testing.T) {
	if _, _, err := temporal.ParseURL("nats://localhost:4222"); err == nil {
		t.Fatal("expected error for non-temporal scheme")
	}
}

func TestNormalizeConfigDefaults(t *testing.T) {
	c := temporal.NormalizeConfig(nil)
	if c.Namespace != "default" || c.LockWorkflowID != "golang_migrate_lock" ||
		c.TaskQueue != "golang_migrate" || c.LockTTL != 5*time.Minute {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

func TestWithInstanceNilClient(t *testing.T) {
	if _, err := temporal.WithInstance(nil, nil); err == nil {
		t.Fatal("expected error for nil client")
	}
}
