package temporal

import (
	"context"
	"fmt"
	"time"

	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// Drop wipes the target namespace: deletes all schedules, terminates all open
// workflow executions, then deletes the namespace itself. Per the
// database.Driver contract this is destructive; the caller must Open() again
// against a fresh namespace to reuse the driver.
func (d *Driver) Drop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wf := d.client.WorkflowService()
	ns := d.config.Namespace

	// 1. Delete schedules.
	var schedToken []byte
	for {
		resp, err := wf.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace: ns, NextPageToken: schedToken,
		})
		if err != nil {
			return fmt.Errorf("temporal: list schedules: %w", err)
		}

		for _, s := range resp.GetSchedules() {
			if _, err := wf.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
				Namespace: ns, ScheduleId: s.GetScheduleId(),
			}); err != nil {
				return fmt.Errorf("temporal: delete schedule %q: %w", s.GetScheduleId(), err)
			}
		}

		schedToken = resp.GetNextPageToken()
		if len(schedToken) == 0 {
			break
		}
	}

	// 2. Terminate open workflow executions.
	var wfToken []byte
	for {
		resp, err := wf.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: ns, Query: "ExecutionStatus='Running'", NextPageToken: wfToken,
		})
		if err != nil {
			return fmt.Errorf("temporal: list workflows: %w", err)
		}

		for _, e := range resp.GetExecutions() {
			id := e.GetExecution().GetWorkflowId()
			if err := d.client.TerminateWorkflow(ctx, id, "", "migrate drop"); err != nil {
				return fmt.Errorf("temporal: terminate %q: %w", id, err)
			}
		}

		wfToken = resp.GetNextPageToken()
		if len(wfToken) == 0 {
			break
		}
	}

	// 3. Delete the namespace.
	if _, err := d.client.OperatorService().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: ns,
	}); err != nil {
		return fmt.Errorf("temporal: delete namespace %q: %w", ns, err)
	}

	d.locked.Store(false)

	return nil
}
