# Neo4j 4.4 CE Data Migration Script

Migrate data between two Neo4j 4.4 Community Edition instances with zero downtime.

## Features

- ✅ **Live Migration** - No downtime required
- ✅ **Batched Operations** - Handles large datasets efficiently
- ✅ **Progress Tracking** - Visual progress bars with tqdm
- ✅ **Error Handling** - Automatic retries for transient errors
- ✅ **Dry Run Mode** - Test without making changes
- ✅ **Selective Migration** - Migrate specific labels/relationships
- ✅ **Schema Migration** - Copies indexes and constraints

## Prerequisites

### Source Server
- Neo4j 4.4 Community Edition
- APOC plugin installed (optional but recommended)
- Network access from migration host

### Target Server
- Neo4j 4.4 Community Edition
- Empty or existing database (will merge data)
- Network access from migration host

### Migration Host
- Python 3.7+
- Network access to both Neo4j servers

## Installation

```bash
# Clone or download the project
cd Neo4jDataMigration

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### Option 1: Environment Variables (Recommended for Production)

Create a `.env` file:

```env
# Source Database
SOURCE_NEO4J_URI=bolt://source-server:7687
SOURCE_NEO4J_USER=neo4j
SOURCE_NEO4J_PASSWORD=your-source-password
SOURCE_NEO4J_DATABASE=neo4j

# Target Database
TARGET_NEO4J_URI=bolt://target-server:7687
TARGET_NEO4J_USER=neo4j
TARGET_NEO4J_PASSWORD=your-target-password
TARGET_NEO4J_DATABASE=neo4j

# Migration Settings
MIGRATION_BATCH_SIZE=1000
MIGRATION_DRY_RUN=false
```

### Option 2: Edit config.py

Update the values directly in `config.py`:

```python
SOURCE_URI = "bolt://source-server:7687"
SOURCE_USER = "neo4j"
SOURCE_PASSWORD = "your-password"
# ... etc
```

## Usage

### Basic Migration

```bash
# Full migration
python migrate.py
```

### Incremental Mode (Default)

By default, the script runs in **incremental mode**, which only migrates nodes that **don't already exist** in the target database. This is perfect for:
- Syncing new data from source to target
- Avoiding duplicates when target already has some data
- Incremental/delta migrations

```bash
# Incremental migration (default - only new nodes)
python migrate.py

# Explicitly specify incremental mode
python migrate.py --incremental

# Specify which property to use as unique identifier
python migrate.py --identifier-property uuid

# Full migration (all nodes, may create duplicates)
python migrate.py --full
```

**How it works:**
1. Auto-detects a unique identifier property (checks constraints, then common names like `id`, `uuid`, etc.)
2. Fetches existing node identifiers from target
3. Filters out nodes that already exist
4. Only creates new nodes

```bash
# Dry run (test without changes)
python migrate.py --dry-run

# Verbose output
python migrate.py --verbose
```

### Selective Migration

Edit `config.py` to specify which labels/relationships to migrate:

```python
LABELS_TO_MIGRATE = ["User", "Product", "Order"]
RELATIONSHIP_TYPES_TO_MIGRATE = ["PURCHASED", "CREATED"]
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Run without making changes to target |
| `--incremental` | Only migrate nodes not present in target (default mode) |
| `--full` | Migrate all data (may create duplicates) |
| `--identifier-property NAME` | Property to use as unique identifier (auto-detected if not specified) |
| `--skip-schema` | Skip index/constraint migration |
| `--skip-nodes` | Skip node migration |
| `--skip-rels` | Skip relationship migration |
| `--skip-cleanup` | Keep temporary `_source_id` properties |
| `--skip-verify` | Skip verification step |
| `--batch-size N` | Set batch size (default: 1000) |
| `--verbose, -v` | Enable debug logging |

## Migration Process

```
┌─────────────────────────────────────────────────────────────┐
│                     MIGRATION FLOW                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. CONNECT                                                 │
│     └── Verify connectivity to source and target            │
│                                                             │
│  2. SCHEMA MIGRATION                                        │
│     ├── Export indexes from source                          │
│     ├── Export constraints from source                      │
│     └── Create on target                                    │
│                                                             │
│  3. NODE MIGRATION                                          │
│     ├── For each label:                                     │
│     │   ├── Read batch from source                          │
│     │   └── Write batch to target                           │
│     └── Track progress with _source_id                      │
│                                                             │
│  4. RELATIONSHIP MIGRATION                                  │
│     ├── For each relationship type:                         │
│     │   ├── Read batch from source                          │
│     │   └── Create on target (match by _source_id)          │
│     └── Progress tracking                                   │
│                                                             │
│  5. CLEANUP                                                 │
│     └── Remove _source_id properties                        │
│                                                             │
│  6. VERIFICATION                                            │
│     ├── Compare node counts                                 │
│     └── Compare relationship counts                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ServiceUnavailable` | Server unreachable | Check network, firewall, port 7687 |
| `AuthError` | Invalid credentials | Verify username/password |
| `MemoryError` | Batch too large | Reduce `--batch-size` |
| `ConnectionTimeout` | Slow network | Increase timeout in config |
| `ConstraintValidationFailed` | Duplicate data | Check existing data on target |

### Performance Tuning

- **Large databases**: Reduce `BATCH_SIZE` to 500 or less
- **Slow network**: Increase `CONNECTION_TIMEOUT`
- **Memory issues**: Reduce batch size, run on server with more RAM

## Pre-Migration Checklist

- [ ] Verify Neo4j versions on both servers (`neo4j version`)
- [ ] Check network connectivity (can ping both servers)
- [ ] Verify credentials work (test with `cypher-shell`)
- [ ] Check disk space on target server
- [ ] Document source database size
- [ ] Run with `--dry-run` first

## Project Structure

```
Neo4jDataMigration/
├── spec/
│   └── migration_plan.md   # Detailed migration plan
├── utils/
│   └── neo4j_utils.py      # Helper functions
├── migrate.py              # Main migration script
├── config.py               # Configuration
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## License

MIT License
