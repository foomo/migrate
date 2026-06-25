package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Op is a single operation applied during Run. Request carries the protojson
// body decoded into the API request type selected by Kind. For Kind "raw",
// Service+Method choose the gRPC method (see raw.go).
type Op struct {
	Kind    string          `json:"op"`
	Request json.RawMessage `json:"request,omitempty"`
	Service string          `json:"service,omitempty"`
	Method  string          `json:"method,omitempty"`
}

const (
	opRegisterNamespace = "register_namespace"
	opUpdateNamespace   = "update_namespace"
	opDeleteNamespace   = "delete_namespace"
	opCreateSchedule    = "create_schedule"
	opUpdateSchedule    = "update_schedule"
	opDeleteSchedule    = "delete_schedule"
	opRaw               = "raw"
)

// Run reads ops and applies each in order. The body is either a bare JSON
// array of ops or an object {"$schema": "...", "ops": [...]} (the object form
// lets editors reference migration.schema.json inline). Empty body = no-op.
func (d *Driver) Run(migration io.Reader) error {
	raw, err := io.ReadAll(migration)
	if err != nil {
		return fmt.Errorf("temporal: read migration: %w", err)
	}

	trimmed := skipWhitespace(raw)
	if len(trimmed) == 0 {
		return nil
	}

	var ops []Op
	if trimmed[0] == '{' {
		var doc struct {
			Ops []Op `json:"ops"`
		}
		if err := json.Unmarshal(trimmed, &doc); err != nil {
			return fmt.Errorf("temporal: parse migration json: %w", err)
		}
		ops = doc.Ops
	} else if err := json.Unmarshal(trimmed, &ops); err != nil {
		return fmt.Errorf("temporal: parse migration json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i, op := range ops {
		if err := d.applyOp(ctx, op); err != nil {
			return fmt.Errorf("temporal: op %d (%s): %w", i, op.Kind, err)
		}
	}

	return nil
}

func (d *Driver) applyOp(ctx context.Context, op Op) error {
	wf := d.client.WorkflowService()
	op2 := d.client.OperatorService()

	switch op.Kind {
	case opRegisterNamespace:
		req := &workflowservice.RegisterNamespaceRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := wf.RegisterNamespace(ctx, req)

		return err
	case opUpdateNamespace:
		req := &workflowservice.UpdateNamespaceRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := wf.UpdateNamespace(ctx, req)

		return err
	case opDeleteNamespace:
		req := &operatorservice.DeleteNamespaceRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := op2.DeleteNamespace(ctx, req)

		return err
	case opCreateSchedule:
		req := &workflowservice.CreateScheduleRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := wf.CreateSchedule(ctx, req)

		return err
	case opUpdateSchedule:
		req := &workflowservice.UpdateScheduleRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := wf.UpdateSchedule(ctx, req)

		return err
	case opDeleteSchedule:
		req := &workflowservice.DeleteScheduleRequest{}
		if err := decodeInto(op.Request, req); err != nil {
			return err
		}

		_, err := wf.DeleteSchedule(ctx, req)

		return err
	case opRaw:
		return d.applyRaw(ctx, op)
	default:
		return fmt.Errorf("unknown op %q", op.Kind)
	}
}

// decodeInto protojson-unmarshals an op request body into a proto message.
func decodeInto(raw json.RawMessage, msg proto.Message) error {
	if len(raw) == 0 {
		return fmt.Errorf("missing request")
	}

	if err := protojson.Unmarshal(raw, msg); err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}

	return nil
}

func skipWhitespace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && isSpace(b[i]) {
		i++
	}

	for j > i && isSpace(b[j-1]) {
		j--
	}

	return b[i:j]
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
