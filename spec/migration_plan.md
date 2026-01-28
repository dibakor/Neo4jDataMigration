# Neo4j 4.4 CE Data Migration Plan

## Overview

Live migration between Neo4j 4.4 CE instances using APOC + Python driver approach.

---

## Approach: APOC + Python (Live Migration)

### Why This Approach

- ✅ No downtime required
- ✅ Supports large datasets with batching
- ✅ Progress tracking and resume capability
- ✅ Better error handling and logging
- ✅ Flexible - supports partial migration

### Prerequisites

- APOC plugin installed on source Neo4j
- Python 3.7+ with `neo4j` driver
- Network connectivity between script host and both Neo4j servers

---

## Script Structure

```
Neo4jDataMigration/
├── spec/
│   └── migration_plan.md     # This file
├── migrate.py                # Main migration script
├── config.py                 # Connection configuration
├── requirements.txt          # Python dependencies
├── README.md                 # Documentation
└── utils/
    └── neo4j_utils.py        # Helper functions
```

---

## Execution Steps

### Phase 1: Pre-Migration
1. Verify Neo4j versions on both servers
2. Install APOC on source Neo4j
3. Check network connectivity
4. Document source database statistics

### Phase 2: Schema Migration
1. Export indexes from source
2. Export constraints from source
3. Create indexes on target
4. Create constraints on target

### Phase 3: Data Migration
1. Migrate nodes by label (batched)
2. Migrate relationships by type (batched)
3. Track progress with checkpoints

### Phase 4: Verification
1. Compare node counts by label
2. Compare relationship counts by type
3. Verify indexes and constraints
4. Spot-check sample data

---

## Potential Errors & Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `ServiceUnavailable` | Server unreachable | Check network/firewall |
| `AuthError` | Invalid credentials | Verify username/password |
| `TransientError` | Temporary DB issue | Retry with backoff |
| `ConstraintValidationFailed` | Duplicate key | Handle duplicates gracefully |
| `MemoryError` | Large batch size | Reduce batch size |
| `ConnectionTimeout` | Slow network | Increase timeout |

---

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BATCH_SIZE` | 1000 | Nodes/relationships per transaction |
| `MAX_RETRIES` | 3 | Retry attempts on transient errors |
| `TIMEOUT` | 60s | Connection timeout |
| `DRY_RUN` | False | Test mode without writing |

---

## Verification Queries

```cypher
-- Node counts by label
MATCH (n) RETURN labels(n), count(n)

-- Relationship counts by type
MATCH ()-[r]->() RETURN type(r), count(r)

-- Index status
SHOW INDEXES

-- Constraint status
SHOW CONSTRAINTS
```
