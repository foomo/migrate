package temporal

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// applyRaw dispatches an arbitrary gRPC method by name on either the
// WorkflowService or OperatorService client.
//
// ponytail: reflection over the gRPC client interface — ceiling is that method
// signatures must be (ctx, *Req, ...opts) (*Resp, error), which is the uniform
// shape of every generated Temporal service method. Upgrade path: replace with
// a generated method map if reflection ever proves too loose.
func (d *Driver) applyRaw(ctx context.Context, op Op) error {
	var svc reflect.Value

	switch op.Service {
	case "workflow", "":
		svc = reflect.ValueOf(d.client.WorkflowService())
	case "operator":
		svc = reflect.ValueOf(d.client.OperatorService())
	default:
		return fmt.Errorf("unknown service %q (want \"workflow\" or \"operator\")", op.Service)
	}

	method := svc.MethodByName(op.Method)
	if !method.IsValid() {
		return fmt.Errorf("unknown method %q on %s service", op.Method, op.Service)
	}

	mt := method.Type()
	// Expect (ctx, *Req, ...grpc.CallOption) (*Resp, error).
	if mt.NumIn() < 2 {
		return fmt.Errorf("method %q has unexpected signature", op.Method)
	}

	reqType := mt.In(1) // *Req
	if reqType.Kind() != reflect.Pointer {
		return fmt.Errorf("method %q request is not a pointer", op.Method)
	}

	reqPtr := reflect.New(reqType.Elem()) // *Req

	msg, ok := reqPtr.Interface().(proto.Message)
	if !ok {
		return fmt.Errorf("method %q request is not a proto.Message", op.Method)
	}

	if err := decodeInto(op.Request, msg); err != nil {
		return err
	}

	out := method.Call([]reflect.Value{reflect.ValueOf(ctx), reqPtr})
	if len(out) == 0 {
		return fmt.Errorf("method %q returned no values", op.Method)
	}
	// Last return value is error.
	if errVal := out[len(out)-1]; !errVal.IsNil() {
		//nolint:forcetypeassert // reflection: Temporal gRPC methods always return error as last value
		return errVal.Interface().(error)
	}

	return nil
}
