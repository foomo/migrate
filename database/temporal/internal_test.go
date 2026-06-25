package temporal_test

import (
	"context"
	"testing"
	"time"

	tctemporal "github.com/foomo/testcontainers-go/modules/temporal"
	"go.temporal.io/sdk/client"
)

// startTemporal boots a Temporal dev-server container (temporalio/temporal:latest
// runs "server start-dev" — in-memory, "default" namespace auto-registered) and
// returns a connected client. Container and client are torn down on cleanup.
// Requires Docker.
func startTemporal(t *testing.T) client.Client {
	t.Helper()

	ctx := context.Background()

	ctr, err := tctemporal.Run(ctx, "temporalio/temporal:latest")
	if err != nil {
		t.Fatalf("start temporal container: %v", err)
	}

	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = ctr.Terminate(cctx)
	})

	hostPort, err := ctr.HostPort(ctx)
	if err != nil {
		t.Fatalf("host port: %v", err)
	}

	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(c.Close)

	return c
}
