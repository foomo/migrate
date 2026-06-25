# Temporal.io database driver — design

Date: 2026-06-24
Status: approved (pending spec review)

## Goal

Add a golang-migrate `database.Driver` that targets [temporal.io](https://temporal.io),
living at `database/temporal/` as its own Go module, mirroring the existing
`database/nats` driver. Migrations administer Temporal resources (namespaces,
schedules, and arbitrary API calls) instead of running SQL.

## Module layout

- New submodule `github.com/foomo/migrate/database/temporal` with its own `go.mod`.
- `database.Register("temporal", &Driver{})` in an `init()` func.
- Files map to interface concerns, same as nats:
  - `driver.go` — `Open` / `WithInstance` / `Close` / `Config`.
  - `run.go` — `Run` + `Op` model + dispatch.
  - `version.go` — `Version` / `SetVersion`.
  - `lock.go` — `Lock` / `Unlock`.
  - `drop.go` — `Drop`.
- The go.work workspace and all Makefile fan-out targets pick the module up
  automatically once it has a `go.mod`.

## Connection

`Open(rawURL)` parses a `temporal://` URL, dials its own `client.Client` via
`client.Dial`, owns it, and closes it on `Close()`.

```
temporal://host:7233/namespace?tls=true&api_key=...&lock_ttl=15s&lock_workflow_id=golang_migrate_lock&task_queue=golang_migrate
```

- Scheme must be `temporal`; otherwise error.
- Host:port → `client.Options.HostPort`.
- Path segment → namespace (falls back to `Config.Namespace`, then `default`).
- Query params override `Config` fields.

`WithInstance(c client.Client, cfg *Config)` wraps an already-connected client
and does **not** close it (mirrors the nats `owns` flag: `Close()` only closes
connections that `Open` dialed).

### Config

All fields optional with defaults.

| Field | Default | Purpose |
|-------|---------|---------|
| `Namespace` | `default` | Target namespace; also where version state lives. |
| `LockTTL` | `15s` | `WorkflowExecutionTimeout` of the lock workflow = orphan-lock recovery window. |
| `LockWorkflowID` | `golang_migrate_lock` | Fixed workflow ID used as the mutex. |
| `TaskQueue` | `golang_migrate` | Task queue for the lock workflow (no worker consumes it). |
| `TLS` | `false` | Enable TLS in client options (Temporal Cloud). |
| `APIKey` | `""` | API key credential for Temporal Cloud, if set. |

## Migrations: JSON array of `Op`

A migration body is a JSON array of `Op` objects, applied in order. An empty
body is a no-op (lets `.down` files revert nothing). This mirrors the nats
driver exactly.

```go
type Op struct {
    Kind    string          `json:"op"`
    Request json.RawMessage `json:"request,omitempty"` // protojson body for the API request
    Service string          `json:"service,omitempty"` // "workflow" | "operator" (raw only)
    Method  string          `json:"method,omitempty"`  // gRPC method name (raw only)
}
```

### Unified dispatch mechanism

Each op carries a **raw JSON `request`** that is `protojson`-unmarshaled into the
matching Temporal API protobuf request type, then handed to the gRPC client.
Because the request is forwarded as-is, new upstream proto fields work without
code changes — the same philosophy as nats forwarding raw stream/consumer
configs into `nats.go` types.

| `op` | gRPC call | request type |
|------|-----------|--------------|
| `register_namespace` | `WorkflowService.RegisterNamespace` | `workflowservice.RegisterNamespaceRequest` |
| `update_namespace` | `WorkflowService.UpdateNamespace` | `workflowservice.UpdateNamespaceRequest` |
| `delete_namespace` | `OperatorService.DeleteNamespace` | `operatorservice.DeleteNamespaceRequest` |
| `create_schedule` | `WorkflowService.CreateSchedule` | `workflowservice.CreateScheduleRequest` |
| `update_schedule` | `WorkflowService.UpdateSchedule` | `workflowservice.UpdateScheduleRequest` |
| `delete_schedule` | `WorkflowService.DeleteSchedule` | `workflowservice.DeleteScheduleRequest` |
| `raw` | `{service, method}` dispatched reflectively | inferred from the method signature |

The named ops give validation and ergonomics. `raw` is the flexible escape
hatch: `service` selects `WorkflowService` or `OperatorService`, `method` names
the gRPC method, and `request` is protojson for whatever that method takes. It
is implemented with reflection over the gRPC client interface
(`MethodByName` → second parameter is the request proto → `reflect.New` →
`protojson.Unmarshal` → `Call`), roughly 30 lines, marked with a `ponytail:`
comment naming the reflection ceiling.

Unknown `op` → error `unknown op %q`, matching nats.

`Run` reads the whole body, trims whitespace, returns early on empty, unmarshals
the array, and applies each op under a 60s context, wrapping errors as
`temporal: op %d (%s): %w`.

## State storage

Chosen approach: **namespace `Data` map for version, a parked workflow for the
lock.** Both are worker-free and survive Temporal's retention period, which only
deletes *closed* workflow executions — open workflows and namespace metadata are
never auto-purged.

### Version + dirty — namespace `Data` map

- `SetVersion(v, dirty)` → `WorkflowService.UpdateNamespace` setting
  `UpdateNamespaceInfo.Data["schema_version"] = itoa(v)` and `["dirty"] = bool`.
  `v == database.NilVersion` (-1) removes both keys (set to empty / cleared).
- `Version()` → `WorkflowService.DescribeNamespace`, read
  `NamespaceInfo.Data["schema_version"]` and `["dirty"]`. Missing keys or
  `NamespaceNotFound` → `(database.NilVersion, false, nil)`.

### Lock — parked fixed-ID workflow

- `Lock()` → `client.ExecuteWorkflow` with:
  - `ID = Config.LockWorkflowID`
  - `WorkflowIdConflictPolicy = FAIL` → if a lock workflow is already open, the
    start returns `serviceerror.WorkflowExecutionAlreadyStarted`, mapped to
    `database.ErrLocked`.
  - `WorkflowExecutionTimeout = Config.LockTTL` → if a migrate process crashes
    without unlocking, the workflow times out, closes, and the next `Lock()`
    succeeds (orphan recovery).
  - `TaskQueue = Config.TaskQueue`, `WorkflowType = "GolangMigrateLock"` (string;
    no worker registered, so it parks holding the ID — exactly what we want).
  - A local `atomic.Bool` guards against double-lock in-process (mirrors nats).
- `Unlock()` → `client.TerminateWorkflow(LockWorkflowID, "", "migrate unlock")`.
  Returns `database.ErrNotLocked` if the local mirror shows we don't hold it.

## Drop

Per the `database.Driver` contract, `Drop` is destructive; the chosen scope is
**full**, consistent with deleting the namespace that holds our version state
(the caller must `Open()` against a fresh namespace afterward, same as nats).

Under a 60s context:
1. `ListSchedules` → `DeleteSchedule` for each.
2. `ListWorkflowExecutions` (open) → `TerminateWorkflow` for each.
3. `OperatorService.DeleteNamespace(Namespace)`.

## Error handling

- All errors wrapped with a `temporal:` prefix.
- `WorkflowExecutionAlreadyStarted` → `database.ErrLocked`.
- `NamespaceNotFound` during `Version()` → `(NilVersion, false, nil)`.
- Per-call context timeouts: 10s for state ops (lock/version), 60s for Run/Drop.

## Testing

- testcontainers with the `temporalio/auto-setup` image (matches the repo's
  Docker-based convention in CLAUDE.md). Compiled and run with `-tags=safe`.
- Black-box `temporal_test` package per the repo Go testing skill; export any
  unexported symbols needed via an `export_test.go` helper.
- Cases:
  - `Open` / `WithInstance` lifecycle and `Close` ownership semantics.
  - Lock contention: second `Lock()` → `database.ErrLocked`.
  - Orphan-lock recovery: short `LockTTL`, simulate crash, lock re-acquired
    after timeout.
  - Version round-trip through namespace `Data`, including `NilVersion` clear.
  - Each named op kind applies the expected resource.
  - `raw` passthrough invokes an arbitrary method (e.g. `DescribeNamespace`).
  - `Drop` removes schedules + namespace.

## Out of scope (YAGNI)

- First-class SearchAttribute ops — `raw` against `OperatorService` covers them.
- Sentinel-workflow + worker state model — heavier; namespace `Data` + parked
  lock workflow is sufficient and worker-free.
- Per-schedule pause / trigger / backfill ops — add when a real migration needs
  them.

## Release

Per-module tag `database/temporal/vX.Y.Z` via `make tag.submodules`, same as
nats.
