package temporal_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	temporal "github.com/foomo/migrate/database/temporal"
)

func TestDropDeletesNamespace(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	// Create a throwaway namespace, then Drop it.
	mig, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "to_be_dropped"})
	create, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	body := `[{"op":"register_namespace","request":{"namespace":"to_be_dropped","workflowExecutionRetentionPeriod":"86400s"}}]`
	if err := create.Run(strings.NewReader(body)); err != nil {
		t.Fatalf("create ns: %v", err)
	}
	// Namespace registration is async; give it a moment to be describable.
	time.Sleep(2 * time.Second)

	if err := mig.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	// Deletion is async; poll for NotFound / deleted state.
	ctx := context.Background()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: "to_be_dropped"})

		var notFound *serviceerror.NamespaceNotFound
		if err != nil && (strings.Contains(err.Error(), "deleted") || errors.As(err, &notFound)) {
			return // success
		}

		time.Sleep(time.Second)
	}

	t.Fatal("namespace not deleted within deadline")
}
