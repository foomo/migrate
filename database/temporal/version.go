package temporal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

// SetVersion stores version+dirty in the namespace Data map. NilVersion clears.
func (d *Driver) SetVersion(version int, dirty bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data := map[string]string{}
	if version == database.NilVersion {
		data[versionKey] = ""
		data[dirtyKey] = ""
	} else {
		data[versionKey] = strconv.Itoa(version)
		data[dirtyKey] = strconv.FormatBool(dirty)
	}

	_, err := d.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:  d.config.Namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{Data: data},
	})
	if err != nil {
		return fmt.Errorf("temporal: set version: %w", err)
	}

	return nil
}

// Version reads version+dirty from the namespace Data map.
func (d *Driver) Version() (int, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := d.client.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: d.config.Namespace,
	})
	if err != nil {
		var notFound *serviceerror.NamespaceNotFound
		if errors.As(err, &notFound) {
			return database.NilVersion, false, nil
		}

		return 0, false, fmt.Errorf("temporal: read version: %w", err)
	}

	data := resp.GetNamespaceInfo().GetData()

	raw := data[versionKey]
	if raw == "" {
		return database.NilVersion, false, nil
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, fmt.Errorf("temporal: decode version %q: %w", raw, err)
	}

	return v, data[dirtyKey] == "true", nil
}
