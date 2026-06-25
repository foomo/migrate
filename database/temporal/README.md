# database/temporal

A [golang-migrate](https://github.com/golang-migrate/migrate) database driver backed by [Temporal](https://temporal.io/).

The driver versions Temporal namespace topology — namespaces, schedules — using migration files that contain a JSON array of operations. Migration state (current version, dirty flag) is stored in the target namespace's Data map. The distributed lock is a parked workflow with a fixed ID; its `WorkflowExecutionTimeout` acts as the orphan-lock TTL.

## URL

```
temporal://host:7233/namespace?tls=true&api_key=KEY&lock_ttl=5m&lock_workflow_id=golang_migrate_lock&task_queue=golang_migrate
```

| Parameter          | Default                  | Description                                              |
|--------------------|--------------------------|----------------------------------------------------------|
| `tls`              | `false`                  | Enable TLS (required for Temporal Cloud)                 |
| `api_key`          | —                        | API key credential (Temporal Cloud)                      |
| `lock_ttl`         | `5m`                     | Lock workflow execution timeout and orphan-lock recovery window. **Must exceed the duration of the longest migration** — if the lock workflow times out mid-run another process can acquire the lock and run concurrently. Lower it only when all migrations are known-fast and a shorter orphan-lock recovery window is desired. |
| `lock_workflow_id` | `golang_migrate_lock`    | Fixed workflow ID used as the migration mutex            |
| `task_queue`       | `golang_migrate`         | Task queue for the lock workflow (no worker needed)      |

The URL path component sets the namespace (`/orders` → namespace `orders`). Defaults to `default`.

## Migration file format

Migration files are `.up.json` / `.down.json` containing a JSON array of ops. Each op has an `"op"` field selecting the operation and a `"request"` field carrying the [protojson](https://protobuf.dev/programming-guides/proto3/#json)-encoded API request body for the corresponding Temporal gRPC method.

```json
[
  {"op": "register_namespace", "request": { ...RegisterNamespaceRequest... }},
  {"op": "create_schedule",    "request": { ...CreateScheduleRequest... }}
]
```

The body may also be an object so editors can reference the schema inline:

```json
{"$schema": "../../migration.schema.json", "ops": [ ...same ops... ]}
```

An empty body (or all-whitespace) is a no-op — useful for `.down` files that should revert nothing.

[`migration.schema.json`](migration.schema.json) describes both forms; `make schema` validates the example migrations against it.

## Op kinds

| Op                   | Temporal API                                        | Notes                                      |
|----------------------|-----------------------------------------------------|--------------------------------------------|
| `register_namespace` | `WorkflowService.RegisterNamespace`                 |                                            |
| `update_namespace`   | `WorkflowService.UpdateNamespace`                   |                                            |
| `delete_namespace`   | `OperatorService.DeleteNamespace`                   |                                            |
| `create_schedule`    | `WorkflowService.CreateSchedule`                    |                                            |
| `update_schedule`    | `WorkflowService.UpdateSchedule`                    |                                            |
| `delete_schedule`    | `WorkflowService.DeleteSchedule`                    |                                            |
| `raw`                | any method (see below)                              | Escape hatch for any gRPC call             |

### `raw` — escape hatch

Use `raw` to call any Temporal gRPC method not covered by the named ops:

```json
{"op": "raw", "service": "workflow", "method": "SignalWorkflowExecution", "request": { ... }}
```

| Field     | Values                     | Description                        |
|-----------|----------------------------|------------------------------------|
| `service` | `"workflow"` (default), `"operator"` | Which gRPC service client to use |
| `method`  | any exported method name   | Exact method name on the service   |
| `request` | protojson object           | Request body for the method        |

## State model

- **Version + dirty flag**: stored in the target namespace's `Data` map under keys `schema_migrations_version` and `schema_migrations_dirty`.
- **Lock**: a workflow started with ID `golang_migrate_lock` (configurable) and `WORKFLOW_ID_CONFLICT_POLICY_FAIL` with `WorkflowExecutionErrorWhenAlreadyStarted: true`. If an already-running workflow is found, `ErrLocked` is returned. The `WorkflowExecutionTimeout` (`lock_ttl`, default `5m`) causes orphaned locks to auto-recover. **`lock_ttl` must exceed the duration of the longest migration**; if it expires mid-run the lock is released and a concurrent process can acquire it.

## Drop

`Drop()` is destructive. It:
1. Deletes all schedules in the namespace.
2. Terminates all running workflow executions.
3. Deletes the namespace itself.

After `Drop()`, call `Open()` against a fresh namespace before reusing the driver.

## Import

Register the driver with a blank import:

```go
import _ "github.com/foomo/migrate/database/temporal"
```

## Example

See [`examples/migrations/`](examples/migrations/) for a sample pair: `0001_init.up.json` registers a namespace and creates a schedule; `0001_init.down.json` reverses both in order.
