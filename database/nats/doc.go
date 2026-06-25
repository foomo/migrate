// Package nats implements a github.com/golang-migrate/migrate/v4 database.Driver
// backed by NATS JetStream.
//
// The driver versions JetStream topology — streams, consumers, KV buckets, and
// KV entries — using migration files that contain a JSON array of operations.
// Migration state (current version, dirty flag, distributed lock) is persisted
// in a JetStream KeyValue bucket.
//
// URL scheme:
//
//	nats://[user:pass@]host:port[,host2:port2]/?bucket=schema_migrations&lock_ttl=15s&replicas=1&domain=<jsDomain>
//
// The driver registers itself under the scheme "nats" in init(), so it becomes
// available via migrate.New("nats://...", ...) after a blank import:
//
//	import _ "manorag.visualstudio.com/ng-ecom/general/service-nats/internal/nats"
//
// Migration file format (.up.json / .down.json):
//
//	[
//	  {"op": "create_stream", "config": { ...jetstream.StreamConfig... }},
//	  {"op": "create_kv",     "config": { ...jetstream.KeyValueConfig... }},
//	  {"op": "create_consumer", "stream": "ORDERS", "config": { ...jetstream.ConsumerConfig... }},
//	  {"op": "kv_put",        "bucket": "settings", "key": "k", "value_b64": "<base64>"}
//	]
//
// The body may also be an object so editors can reference the schema inline:
//
//	{"$schema": "../../migration.schema.json", "ops": [ ...same ops... ]}
//
// migration.schema.json describes both forms; "make schema" validates the
// example migrations against it.
package nats
