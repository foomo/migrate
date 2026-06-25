package nats

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Op is a single operation applied during Run. Exactly one of the
// type-specific fields is consulted per Op based on Kind.
type Op struct {
	Kind string `json:"op"`

	// Stream / KV configs (raw so we forward exact fields to nats.go without
	// re-mapping when the API gains new fields).
	Config json.RawMessage `json:"config,omitempty"`

	Stream   string `json:"stream,omitempty"`
	Bucket   string `json:"bucket,omitempty"`
	Name     string `json:"name,omitempty"`
	Key      string `json:"key,omitempty"`
	ValueB64 string `json:"value_b64,omitempty"`
}

const (
	opCreateStream   = "create_stream"
	opUpdateStream   = "update_stream"
	opDeleteStream   = "delete_stream"
	opCreateConsumer = "create_consumer"
	opUpdateConsumer = "update_consumer"
	opDeleteConsumer = "delete_consumer"
	opCreateKV       = "create_kv"
	opUpdateKV       = "update_kv"
	opDeleteKV       = "delete_kv"
	opKVPut          = "kv_put"
	opKVDelete       = "kv_delete"
)

// Run reads ops from the reader and applies each in order. The body is either
// a bare JSON array of ops or an object {"$schema": "...", "ops": [...]} (the
// object form lets editors reference migration.schema.json inline). An empty
// body is a no-op so callers can use ".down" files that revert nothing (e.g. a
// migration that only seeded data and chose not to undo it).
func (d *Driver) Run(migration io.Reader) error {
	raw, err := io.ReadAll(migration)
	if err != nil {
		return fmt.Errorf("nats: read migration: %w", err)
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
			return fmt.Errorf("nats: parse migration json: %w", err)
		}
		ops = doc.Ops
	} else if err := json.Unmarshal(trimmed, &ops); err != nil {
		return fmt.Errorf("nats: parse migration json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i, op := range ops {
		if err := d.applyOp(ctx, op); err != nil {
			return fmt.Errorf("nats: op %d (%s): %w", i, op.Kind, err)
		}
	}
	return nil
}

func (d *Driver) applyOp(ctx context.Context, op Op) error {
	switch op.Kind {
	case opCreateStream:
		cfg, err := decodeStreamConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateStream(ctx, cfg)
		return err
	case opUpdateStream:
		cfg, err := decodeStreamConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateOrUpdateStream(ctx, cfg)
		return err
	case opDeleteStream:
		if op.Name == "" {
			return fmt.Errorf("missing name")
		}
		return d.js.DeleteStream(ctx, op.Name)

	case opCreateConsumer:
		if op.Stream == "" {
			return fmt.Errorf("missing stream")
		}
		cfg, err := decodeConsumerConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateConsumer(ctx, op.Stream, cfg)
		return err
	case opUpdateConsumer:
		if op.Stream == "" {
			return fmt.Errorf("missing stream")
		}
		cfg, err := decodeConsumerConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateOrUpdateConsumer(ctx, op.Stream, cfg)
		return err
	case opDeleteConsumer:
		if op.Stream == "" || op.Name == "" {
			return fmt.Errorf("missing stream or name")
		}
		return d.js.DeleteConsumer(ctx, op.Stream, op.Name)

	case opCreateKV:
		cfg, err := decodeKVConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateKeyValue(ctx, cfg)
		return err
	case opUpdateKV:
		cfg, err := decodeKVConfig(op.Config)
		if err != nil {
			return err
		}
		_, err = d.js.CreateOrUpdateKeyValue(ctx, cfg)
		return err
	case opDeleteKV:
		if op.Bucket == "" {
			return fmt.Errorf("missing bucket")
		}
		return d.js.DeleteKeyValue(ctx, op.Bucket)

	case opKVPut:
		if op.Bucket == "" || op.Key == "" {
			return fmt.Errorf("missing bucket or key")
		}
		val, err := base64.StdEncoding.DecodeString(op.ValueB64)
		if err != nil {
			return fmt.Errorf("invalid value_b64: %w", err)
		}
		kv, err := d.js.KeyValue(ctx, op.Bucket)
		if err != nil {
			return err
		}
		_, err = kv.Put(ctx, op.Key, val)
		return err
	case opKVDelete:
		if op.Bucket == "" || op.Key == "" {
			return fmt.Errorf("missing bucket or key")
		}
		kv, err := d.js.KeyValue(ctx, op.Bucket)
		if err != nil {
			return err
		}
		return kv.Delete(ctx, op.Key)

	default:
		return fmt.Errorf("unknown op %q", op.Kind)
	}
}

func decodeStreamConfig(raw json.RawMessage) (jetstream.StreamConfig, error) {
	var cfg jetstream.StreamConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("missing config")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid stream config: %w", err)
	}
	return cfg, nil
}

func decodeConsumerConfig(raw json.RawMessage) (jetstream.ConsumerConfig, error) {
	var cfg jetstream.ConsumerConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("missing config")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid consumer config: %w", err)
	}
	return cfg, nil
}

func decodeKVConfig(raw json.RawMessage) (jetstream.KeyValueConfig, error) {
	var cfg jetstream.KeyValueConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("missing config")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid kv config: %w", err)
	}
	return cfg, nil
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

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
