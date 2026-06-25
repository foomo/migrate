package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// lockWorkflowType is the workflow type name started to hold the lock. No
// worker is registered for it; the execution parks holding the workflow ID.
const lockWorkflowType = "GolangMigrateLock"

// Lock acquires the migration lock by starting a fixed-ID workflow with a
// conflict policy that fails if one is already running. The execution stays
// open while the lock is held; WorkflowExecutionTimeout (LockTTL) closes it if
// a migrate process crashes, so the lock auto-recovers.
func (d *Driver) Lock() error {
	if !d.locked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       d.config.LockWorkflowID,
		TaskQueue:                                d.config.TaskQueue,
		WorkflowExecutionTimeout:                 d.config.LockTTL,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, lockWorkflowType)
	if err != nil {
		d.locked.Store(false)

		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return database.ErrLocked
		}

		return fmt.Errorf("temporal: acquire lock: %w", err)
	}

	return nil
}

// Unlock releases the lock by terminating the lock workflow.
func (d *Driver) Unlock() error {
	if !d.locked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := d.client.TerminateWorkflow(ctx, d.config.LockWorkflowID, "", "migrate unlock"); err != nil {
		// Already gone (timed out / terminated) is fine.
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}

		return fmt.Errorf("temporal: release lock: %w", err)
	}

	return nil
}
