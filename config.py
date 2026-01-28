"""
Neo4j Migration Configuration

Copy this file to config_local.py and update with your actual credentials.
Do NOT commit config_local.py to version control.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# =============================================================================
# SOURCE DATABASE CONFIGURATION (Neo4j 4.4 CE)
# =============================================================================
SOURCE_URI = os.getenv("SOURCE_NEO4J_URI", "bolt://source-server:7687")
SOURCE_USER = os.getenv("SOURCE_NEO4J_USER", "neo4j")
SOURCE_PASSWORD = os.getenv("SOURCE_NEO4J_PASSWORD", "your-source-password")
SOURCE_DATABASE = os.getenv("SOURCE_NEO4J_DATABASE", "neo4j")

# =============================================================================
# TARGET DATABASE CONFIGURATION (Neo4j 4.4 CE)
# =============================================================================
TARGET_URI = os.getenv("TARGET_NEO4J_URI", "bolt://target-server:7687")
TARGET_USER = os.getenv("TARGET_NEO4J_USER", "neo4j")
TARGET_PASSWORD = os.getenv("TARGET_NEO4J_PASSWORD", "your-target-password")
TARGET_DATABASE = os.getenv("TARGET_NEO4J_DATABASE", "neo4j")

# =============================================================================
# MIGRATION SETTINGS
# =============================================================================

# Batch size for node/relationship migration
# - For datasets < 1M nodes: 1000-5000 works well
# - For datasets > 100M nodes (like 114M): 500-1000 recommended
# - Reduce if you encounter memory issues or timeouts
BATCH_SIZE = int(os.getenv("MIGRATION_BATCH_SIZE", "500"))

# Maximum retries for transient errors
MAX_RETRIES = int(os.getenv("MIGRATION_MAX_RETRIES", "3"))

# Connection timeout in seconds
CONNECTION_TIMEOUT = int(os.getenv("MIGRATION_TIMEOUT", "60"))

# Enable dry-run mode (no changes to target)
DRY_RUN = os.getenv("MIGRATION_DRY_RUN", "false").lower() == "true"

# Enable verbose logging
VERBOSE = os.getenv("MIGRATION_VERBOSE", "true").lower() == "true"

# =============================================================================
# OPTIONAL: SELECTIVE MIGRATION
# =============================================================================

# Specify labels to migrate (empty list = all labels)
# Example: ["User", "Product", "Order"]
LABELS_TO_MIGRATE = []

# Specify relationship types to migrate (empty list = all types)
# Example: ["PURCHASED", "FOLLOWS", "CREATED"]
RELATIONSHIP_TYPES_TO_MIGRATE = []

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
LOG_FILE = os.getenv("MIGRATION_LOG_FILE", "migration.log")
LOG_LEVEL = os.getenv("MIGRATION_LOG_LEVEL", "INFO")
