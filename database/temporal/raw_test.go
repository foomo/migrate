package temporal_test

import (
	"strings"
	"testing"

	temporal "github.com/foomo/migrate/database/temporal"
)

// Raw dispatch to DescribeNamespace against the default namespace should succeed.
func TestRawDispatch(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	body := `[
	  {"op":"raw","service":"workflow","method":"DescribeNamespace","request":{"namespace":"default"}}
	]`
	if err := d.Run(strings.NewReader(body)); err != nil {
		t.Fatalf("raw DescribeNamespace: %v", err)
	}
}

func TestRawUnknownMethod(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	body := `[{"op":"raw","service":"workflow","method":"NoSuchMethod","request":{}}]`
	if err := d.Run(strings.NewReader(body)); err == nil {
		t.Fatal("expected error for unknown method")
	}
}

func TestRawUnknownService(t *testing.T) {
	t.Parallel()
	c := startTemporal(t)
	d, _ := temporal.WithInstance(c, &temporal.Config{Namespace: "default"})

	body := `[{"op":"raw","service":"bogus","method":"X","request":{}}]`
	if err := d.Run(strings.NewReader(body)); err == nil {
		t.Fatal("expected error for unknown service")
	}
}
