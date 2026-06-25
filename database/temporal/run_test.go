package temporal_test

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/api/workflowservice/v1"

	temporal "github.com/foomo/migrate/database/temporal"
)

func TestRunEmptyIsNoop(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)

	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})
	if err := d.Run(strings.NewReader("   \n")); err != nil {
		t.Fatalf("empty Run: %v", err)
	}
}

func TestRunRegisterNamespace(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	// Object form (with inline $schema) — array form is covered by the other tests.
	body := `{"$schema":"../../migration.schema.json","ops":[
	  {"op":"register_namespace","request":{"namespace":"created_by_migrate","workflowExecutionRetentionPeriod":"86400s"}}
	]}`
	if err := d.Run(strings.NewReader(body)); err != nil {
		t.Fatalf("Run register_namespace: %v", err)
	}

	ctx := context.Background()

	_, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "created_by_migrate",
	})
	if err != nil {
		t.Fatalf("namespace not created: %v", err)
	}
}

func TestRunUnknownOp(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	err := d.Run(strings.NewReader(`[{"op":"nope"}]`))
	if err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatalf("Run unknown op = %v, want 'unknown op' error", err)
	}
}
