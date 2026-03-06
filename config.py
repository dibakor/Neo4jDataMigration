"""
Neo4j Migration Configuration Loader

Edit config.json to set your credentials and migration settings.
Do NOT commit config.json with real passwords to version control.
"""

import json
import pathlib

_cfg = json.loads((pathlib.Path(__file__).parent / "config.json").read_text())

# Source database
SOURCE_URI = _cfg["source"]["uri"]
SOURCE_USER = _cfg["source"]["user"]
SOURCE_PASSWORD = _cfg["source"]["password"]
SOURCE_DATABASE = _cfg["source"]["database"]

# Target database
TARGET_URI = _cfg["target"]["uri"]
TARGET_USER = _cfg["target"]["user"]
TARGET_PASSWORD = _cfg["target"]["password"]
TARGET_DATABASE = _cfg["target"]["database"]

# Migration settings
BATCH_SIZE = _cfg["migration"]["batch_size"]
MAX_RETRIES = _cfg["migration"]["max_retries"]
CONNECTION_TIMEOUT = _cfg["migration"]["connection_timeout"]
DRY_RUN = _cfg["migration"]["dry_run"]
VERBOSE = _cfg["migration"]["verbose"]
LABELS_TO_MIGRATE = _cfg["migration"]["labels_to_migrate"]
RELATIONSHIP_TYPES_TO_MIGRATE = _cfg["migration"]["relationship_types_to_migrate"]

# Logging
LOG_FILE = _cfg["logging"]["log_file"]
LOG_LEVEL = _cfg["logging"]["log_level"]
