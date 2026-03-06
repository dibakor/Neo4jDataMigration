# Neo4j Data Migration — Documentation

Migrates data between two Neo4j 4.4 CE instances. Supports batched operations, incremental mode, schema migration, checkpointing, and verification.

---

## Quick Start

```bash
pip install -r requirements.txt

# Edit config.json with your connection details
python migrate.py

# Dry run (no writes)
python migrate.py --dry-run

# Full migration (no deduplication)
python migrate.py --full

# Resume after interruption
python migrate.py --resume
```

---

## Configuration (`config.json`)

All settings live in `config.json`. Edit this file before running the migration. Do not commit real credentials to version control.

```json
{
  "source": {
    "uri": "bolt://source-server:7687",
    "user": "neo4j",
    "password": "your-source-password",
    "database": "neo4j"
  },
  "target": {
    "uri": "bolt://target-server:7687",
    "user": "neo4j",
    "password": "your-target-password",
    "database": "neo4j"
  },
  "migration": {
    "batch_size": 500,
    "max_retries": 3,
    "connection_timeout": 60,
    "dry_run": false,
    "verbose": false,
    "labels_to_migrate": [],
    "relationship_types_to_migrate": []
  },
  "logging": {
    "log_file": "migration.log",
    "log_level": "INFO"
  }
}
```

### Configuration Fields

| Section | Field | Description |
|---|---|---|
| `source` / `target` | `uri` | Bolt URI of the Neo4j instance |
| | `user` | Database username |
| | `password` | Database password |
| | `database` | Database name (usually `neo4j`) |
| `migration` | `batch_size` | Nodes/relationships processed per batch |
| | `max_retries` | Retry attempts on transient failures |
| | `connection_timeout` | Connection timeout in seconds |
| | `dry_run` | If `true`, reads data but writes nothing |
| | `verbose` | If `true`, enables DEBUG-level logging |
| | `labels_to_migrate` | List of node labels to migrate. Empty = all labels |
| | `relationship_types_to_migrate` | List of relationship types to migrate. Empty = all types |
| `logging` | `log_file` | Path to log file |
| | `log_level` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

---

## CLI Options (`migrate.py`)

| Flag | Description |
|---|---|
| `--dry-run` | Run without writing anything to the target |
| `--incremental` | Only migrate nodes not already in the target (default) |
| `--full` | Migrate all nodes regardless of target state (may create duplicates) |
| `--skip-schema` | Skip index and constraint migration |
| `--skip-nodes` | Skip node migration |
| `--skip-rels` | Skip relationship migration |
| `--skip-cleanup` | Skip removal of temporary `_source_id` properties |
| `--skip-verify` | Skip post-migration verification |
| `--batch-size N` | Override batch size from config |
| `--verbose` / `-v` | Enable verbose (DEBUG) output |
| `--resume` | Resume from the last checkpoint |
| `--no-checkpoint` | Disable checkpointing |
| `--reset` | Clear checkpoint and start fresh |
| `--identifier-property` | Property to use as the unique node identifier (auto-detected if omitted) |

---

## Migration Pipeline

The migration runs in four sequential phases:

1. **Schema** — Copies indexes and constraints from source to target.
2. **Nodes** — Migrates nodes by label in batches. In incremental mode, nodes that already exist in the target are skipped.
3. **Relationships** — Migrates relationships by type in batches, matching endpoints via the temporary `_source_id` property.
4. **Cleanup** — Removes the temporary `_source_id` property from all target nodes.
5. **Verification** — Compares node and relationship counts between source and target.

---

## `migrate.py` — Functions

### `setup_logging(verbose, log_file)`
Configures logging to stdout and optionally to a file. Sets level to `DEBUG` when `verbose=True`, otherwise `INFO`.

### `MigrationProgress`
Tracks migration statistics throughout a run.

| Attribute | Description |
|---|---|
| `nodes_migrated` | Count of nodes written to target |
| `nodes_skipped` | Count of nodes skipped (already existed) |
| `relationships_migrated` | Count of relationships written |
| `errors` | List of error messages |
| `warnings` | List of warning messages |

| Method | Description |
|---|---|
| `start()` | Records start time and logs it |
| `finish()` | Records end time and logs duration |
| `add_error(msg)` | Appends to errors list and logs at ERROR level |
| `add_warning(msg)` | Appends to warnings list and logs at WARNING level |
| `summary()` | Returns a formatted string summary of the migration |

### `migrate_schema(source, target, dry_run)`
Copies all non-LOOKUP indexes and UNIQUE constraints from source to target. Skips objects that already exist. Logs warnings on failure without stopping the migration.

### `migrate_nodes(source, target, labels, batch_size, progress, dry_run, incremental, checkpoint)`
Iterates over each label and migrates nodes in batches.

- **Incremental mode**: Auto-detects a unique identifier property (from constraints, indexes, or common names like `id`, `uuid`). Uses database-side filtering to skip nodes that already exist in the target.
- **Full mode**: Creates all nodes without deduplication.
- Supports checkpoint-based resumption — skips labels already marked complete.
- Uses `tqdm` progress bars with ETA per label.

### `migrate_relationships(source, target, rel_types, batch_size, progress, dry_run)`
Iterates over each relationship type and migrates relationships in batches. Matches source and target nodes via the `_source_id` property set during node migration.

### `verify_migration(source, target, progress)`
Compares per-label node counts and per-type relationship counts between source and target. Adds a warning for each mismatch. Returns `True` if all counts match, `False` otherwise.

### `main()`
Entry point. Parses CLI arguments, sets up logging, establishes connections, and orchestrates all migration phases.

---

## `utils/neo4j_utils.py` — Functions

### `Neo4jConnection`
Manages a Neo4j driver with retry logic.

| Method | Description |
|---|---|
| `connect()` | Opens the driver and verifies connectivity |
| `close()` | Closes the driver |
| `get_session()` | Returns a new session for the configured database |
| `execute_with_retry(query, parameters, write)` | Runs a Cypher query with exponential-backoff retry on transient errors. Use `write=True` for write transactions. |

Supports use as a context manager (`with Neo4jConnection(...) as conn:`).

### Schema Functions

| Function | Description |
|---|---|
| `get_all_labels(conn)` | Returns all node labels in the database |
| `get_all_relationship_types(conn)` | Returns all relationship types |
| `get_indexes(conn)` | Returns all ONLINE indexes (name, type, labelsOrTypes, properties) |
| `get_constraints(conn)` | Returns all constraints (name, type, labelsOrTypes, properties) |
| `create_index(conn, label, properties, index_type, dry_run)` | Creates a BTREE index. No-op if it already exists |
| `create_constraint(conn, label, properties, constraint_type, dry_run)` | Creates a UNIQUENESS constraint. No-op if it already exists |

### Count Functions

| Function | Description |
|---|---|
| `get_node_count(conn, label)` | Count of nodes, optionally filtered by label |
| `get_relationship_count(conn, rel_type)` | Count of relationships, optionally filtered by type |
| `get_database_stats(conn)` | Returns total nodes, total relationships, and counts per label and relationship type |
| `get_existing_node_count(conn, label)` | Count of nodes with a specific label (used in incremental mode logging) |

### Node Migration Functions

| Function | Description |
|---|---|
| `get_nodes_batch(conn, label, skip, limit)` | Fetches a batch of nodes ordered by internal ID |
| `get_node_identifier_property(conn, label)` | Auto-detects the best unique identifier property. Checks unique constraints first, then common names (`id`, `uuid`, `uid`, `_id`, `key`, `code`) |
| `get_existing_node_identifiers(conn, label, identifier_prop)` | Returns all existing identifier values from the target. Suitable for small datasets only |
| `check_nodes_exist_batch(conn, label, identifier_prop, identifiers)` | Checks which of the given identifiers already exist in the target. Returns the set that exist |
| `filter_new_nodes(nodes, existing_identifiers, identifier_prop)` | In-memory filter — returns only nodes whose identifier is not in `existing_identifiers` |
| `filter_new_nodes_via_db(target_conn, label, identifier_prop, nodes)` | Database-side filter. Checks existence in the target and returns `(new_nodes, skipped_count)`. Preferred for large datasets |
| `create_nodes_batch(conn, label, nodes, dry_run)` | Creates nodes in bulk using `UNWIND`. Sets a temporary `_source_id` property for relationship matching |
| `merge_nodes_batch(conn, label, nodes, identifier_prop, dry_run)` | Uses `MERGE` to create or skip nodes. Returns `(created, skipped)` counts |
| `cleanup_merge_flags(conn, dry_run)` | Removes the temporary `_was_created` property set during merge |

### Relationship Migration Functions

| Function | Description |
|---|---|
| `get_relationships_batch(conn, rel_type, skip, limit)` | Fetches a batch of relationships with endpoint IDs, labels, and properties |
| `create_relationships_batch(conn, rel_type, relationships, dry_run)` | Creates relationships by matching endpoints via `_source_id`. Returns count created |
| `cleanup_source_ids(conn, dry_run)` | Removes `_source_id` from all nodes. Run after relationship migration is complete |

### Custom Exceptions

| Exception | Description |
|---|---|
| `MigrationError` | Base class for all migration exceptions |
| `ConnectionError` | Raised when the Neo4j server is unreachable |
| `AuthenticationError` | Raised on authentication failure |
| `DataIntegrityError` | Raised on constraint violations |

---

## `utils/checkpoint.py` — `CheckpointManager`

Saves migration progress to `.migration_checkpoints/progress.json` so interrupted migrations can resume.

| Method | Description |
|---|---|
| `start_migration()` | Initializes or resumes a checkpoint |
| `is_label_complete(label)` | Returns `True` if the label has been fully migrated |
| `mark_label_complete(label)` | Marks a label as done and saves to disk |
| `get_label_offset(label)` | Returns the batch offset to resume from for a label |
| `update_label_progress(label, offset, migrated, skipped)` | Saves current progress for a label |
| `is_rel_type_complete(rel_type)` | Returns `True` if the relationship type is fully migrated |
| `mark_rel_type_complete(rel_type)` | Marks a relationship type as done |
| `get_rel_type_offset(rel_type)` | Returns the batch offset to resume from for a relationship type |
| `update_rel_type_progress(rel_type, offset, migrated, skipped)` | Saves current progress for a relationship type |
| `get_stats()` | Returns cumulative migration statistics |
| `get_summary()` | Returns a human-readable progress summary string |
| `clear()` | Deletes the checkpoint file and resets all state |

---

## Tests

### `test_e2e_migration.py` — In-Memory Tests
Runs without Docker using an in-memory graph mock. Tests all migration phases against a simulated Neo4j backend.

### `test_e2e_containers.py` — Docker-Based Tests
Spins up real Neo4j 4.4 CE containers using `testcontainers`. Auto-skips if Docker is not running.

```bash
pytest test_e2e_migration.py -v     # In-memory, always runnable
pytest test_e2e_containers.py -v    # Requires Docker
```

| Test Class | What It Tests |
|---|---|
| `TestFullMigration` | Full pipeline (schema + nodes + relationships + verify), property preservation |
| `TestIncrementalMigration` | No duplicates on re-run, delta migration of new nodes |
| `TestSchemaMigration` | Index and constraint migration, idempotency |
| `TestDryRun` | Dry run writes nothing but counts correctly |
| `TestCheckpointResume` | Completed labels are skipped on resume |
| `TestVerification` | Passes on full migration, detects count mismatches |
