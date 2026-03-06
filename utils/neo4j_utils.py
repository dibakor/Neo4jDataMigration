"""
Neo4j Utility Functions for Data Migration

This module provides helper functions for connecting to Neo4j databases
and performing data migration operations with proper error handling.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import (
    ServiceUnavailable,
    AuthError,
    TransientError,
    ConstraintError,
    ClientError,
)

logger = logging.getLogger(__name__)


class Neo4jConnection:
    """Manages a connection to a Neo4j database with retry logic."""

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
        timeout: int = 60,
        max_retries: int = 3,
    ):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.max_retries = max_retries
        self._driver: Optional[Driver] = None

    def connect(self) -> Driver:
        """Establish connection to Neo4j database."""
        if self._driver is None:
            try:
                self._driver = GraphDatabase.driver(
                    self.uri,
                    auth=(self.user, self.password),
                    connection_timeout=self.timeout,
                    max_connection_lifetime=3600,
                )
                # Verify connectivity
                self._driver.verify_connectivity()
                logger.info(f"Connected to Neo4j at {self.uri}")
            except ServiceUnavailable as e:
                logger.error(f"Failed to connect to {self.uri}: {e}")
                raise ConnectionError(
                    f"Neo4j server at {self.uri} is unavailable. "
                    "Check if the server is running and accessible."
                ) from e
            except AuthError as e:
                logger.error(f"Authentication failed for {self.uri}: {e}")
                raise AuthenticationError(
                    f"Authentication failed for {self.uri}. "
                    "Check your username and password."
                ) from e
        return self._driver

    def close(self) -> None:
        """Close the database connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info(f"Closed connection to {self.uri}")

    def get_session(self) -> Session:
        """Get a new session for the database."""
        driver = self.connect()
        return driver.session(database=self.database)

    def execute_with_retry(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        write: bool = False,
    ) -> List[Dict[str, Any]]:
        """Execute a Cypher query with retry logic for transient errors."""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                with self.get_session() as session:
                    if write:
                        result = session.execute_write(
                            lambda tx: list(tx.run(query, parameters or {}))
                        )
                    else:
                        result = session.execute_read(
                            lambda tx: list(tx.run(query, parameters or {}))
                        )
                    return [dict(record) for record in result]

            except TransientError as e:
                last_exception = e
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(
                    f"Transient error on attempt {attempt + 1}/{self.max_retries}. "
                    f"Retrying in {wait_time}s: {e}"
                )
                time.sleep(wait_time)

            except ConstraintError as e:
                logger.error(f"Constraint violation: {e}")
                raise DataIntegrityError(
                    f"Constraint violation during migration: {e}"
                ) from e

            except ClientError as e:
                logger.error(f"Client error: {e}")
                raise MigrationError(f"Query execution failed: {e}") from e

        raise MigrationError(
            f"Query failed after {self.max_retries} attempts: {last_exception}"
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# =============================================================================
# Custom Exceptions
# =============================================================================


class MigrationError(Exception):
    """Base exception for migration errors."""
    pass


class ConnectionError(MigrationError):
    """Exception for connection-related errors."""
    pass


class AuthenticationError(MigrationError):
    """Exception for authentication failures."""
    pass


class DataIntegrityError(MigrationError):
    """Exception for data integrity issues (constraints, duplicates)."""
    pass


# =============================================================================
# Schema Functions
# =============================================================================


def get_all_labels(conn: Neo4jConnection) -> List[str]:
    """Get all node labels from the database."""
    result = conn.execute_with_retry("CALL db.labels() YIELD label RETURN label")
    return [record["label"] for record in result]


def get_all_relationship_types(conn: Neo4jConnection) -> List[str]:
    """Get all relationship types from the database."""
    result = conn.execute_with_retry(
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
    )
    return [record["relationshipType"] for record in result]


def get_indexes(conn: Neo4jConnection) -> List[Dict[str, Any]]:
    """Get all indexes from the database."""
    # Neo4j 4.4 compatible query
    result = conn.execute_with_retry(
        """
        SHOW INDEXES YIELD name, type, labelsOrTypes, properties, state
        WHERE state = 'ONLINE'
        RETURN name, type, labelsOrTypes, properties
        """
    )
    return result


def get_constraints(conn: Neo4jConnection) -> List[Dict[str, Any]]:
    """Get all constraints from the database."""
    # Neo4j 4.4 compatible query
    result = conn.execute_with_retry(
        """
        SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties
        RETURN name, type, labelsOrTypes, properties
        """
    )
    return result


def create_index(
    conn: Neo4jConnection,
    label: str,
    properties: List[str],
    index_type: str = "BTREE",
    dry_run: bool = False,
) -> None:
    """Create an index on the target database."""
    props = ", ".join([f"n.{p}" for p in properties])
    index_name = f"idx_{label}_{'_'.join(properties)}"

    query = f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{label}) ON ({props})"

    if dry_run:
        logger.info(f"[DRY RUN] Would create index: {query}")
        return

    try:
        conn.execute_with_retry(query, write=True)
        logger.info(f"Created index: {index_name}")
    except ClientError as e:
        if "already exists" in str(e).lower():
            logger.info(f"Index {index_name} already exists, skipping")
        else:
            raise


def create_constraint(
    conn: Neo4jConnection,
    label: str,
    properties: List[str],
    constraint_type: str = "UNIQUENESS",
    dry_run: bool = False,
) -> None:
    """Create a constraint on the target database."""
    props = ", ".join([f"n.{p}" for p in properties])
    constraint_name = f"constraint_{label}_{'_'.join(properties)}"

    if constraint_type == "UNIQUENESS":
        query = (
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE ({props}) IS UNIQUE"
        )
    else:
        logger.warning(f"Unsupported constraint type: {constraint_type}")
        return

    if dry_run:
        logger.info(f"[DRY RUN] Would create constraint: {query}")
        return

    try:
        conn.execute_with_retry(query, write=True)
        logger.info(f"Created constraint: {constraint_name}")
    except ClientError as e:
        if "already exists" in str(e).lower():
            logger.info(f"Constraint {constraint_name} already exists, skipping")
        else:
            raise


# =============================================================================
# Count Functions
# =============================================================================


def get_node_count(conn: Neo4jConnection, label: Optional[str] = None) -> int:
    """Get count of nodes, optionally filtered by label."""
    if label:
        query = f"MATCH (n:{label}) RETURN count(n) as count"
    else:
        query = "MATCH (n) RETURN count(n) as count"

    result = conn.execute_with_retry(query)
    return result[0]["count"] if result else 0


def get_relationship_count(
    conn: Neo4jConnection, rel_type: Optional[str] = None
) -> int:
    """Get count of relationships, optionally filtered by type."""
    if rel_type:
        query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
    else:
        query = "MATCH ()-[r]->() RETURN count(r) as count"

    result = conn.execute_with_retry(query)
    return result[0]["count"] if result else 0


def get_database_stats(conn: Neo4jConnection) -> Dict[str, Any]:
    """Get comprehensive database statistics."""
    stats = {
        "total_nodes": get_node_count(conn),
        "total_relationships": get_relationship_count(conn),
        "labels": {},
        "relationship_types": {},
    }

    # Count by label
    for label in get_all_labels(conn):
        stats["labels"][label] = get_node_count(conn, label)

    # Count by relationship type
    for rel_type in get_all_relationship_types(conn):
        stats["relationship_types"][rel_type] = get_relationship_count(conn, rel_type)

    return stats


# =============================================================================
# Node Migration Functions
# =============================================================================


def get_nodes_batch(
    conn: Neo4jConnection,
    label: str,
    skip: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """Get a batch of nodes with their properties."""
    query = f"""
    MATCH (n:{label})
    RETURN id(n) as _id, properties(n) as props
    ORDER BY id(n)
    SKIP $skip
    LIMIT $limit
    """
    return conn.execute_with_retry(query, {"skip": skip, "limit": limit})


def get_node_identifier_property(
    conn: Neo4jConnection,
    label: str,
) -> Optional[str]:
    """
    Determine the best property to use as a unique identifier for a label.
    Checks for unique constraints first, then indexes, then common ID properties.
    """
    # Check for unique constraints
    constraints = get_constraints(conn)
    for constraint in constraints:
        labels = constraint.get("labelsOrTypes", [])
        if label in labels and "UNIQUE" in constraint.get("type", "").upper():
            properties = constraint.get("properties", [])
            if properties:
                return properties[0]
    
    # Check for common identifier properties
    common_ids = ["id", "uuid", "uid", "_id", "ID", "Id", "key", "code"]
    
    # Sample a node to check available properties
    result = conn.execute_with_retry(
        f"MATCH (n:{label}) RETURN keys(n) as props LIMIT 1"
    )
    
    if result and result[0].get("props"):
        node_props = result[0]["props"]
        for common_id in common_ids:
            if common_id in node_props:
                return common_id
    
    return None


def get_existing_node_identifiers(
    conn: Neo4jConnection,
    label: str,
    identifier_prop: str,
) -> set:
    """
    Get existing node identifiers from the target database.
    
    WARNING: For very large databases (>1M nodes), use check_nodes_exist_batch instead
    to avoid loading all IDs into memory.
    """
    query = f"""
    MATCH (n:{label})
    WHERE n.{identifier_prop} IS NOT NULL
    RETURN n.{identifier_prop} as identifier
    """
    result = conn.execute_with_retry(query)
    return {record["identifier"] for record in result}


def get_existing_node_count(
    conn: Neo4jConnection,
    label: str,
) -> int:
    """Get count of existing nodes with a specific label in target."""
    result = conn.execute_with_retry(
        f"MATCH (n:{label}) RETURN count(n) as count"
    )
    return result[0]["count"] if result else 0


def check_nodes_exist_batch(
    conn: Neo4jConnection,
    label: str,
    identifier_prop: str,
    identifiers: List[Any],
) -> set:
    """
    Check which identifiers already exist in the target database.
    Returns the set of identifiers that EXIST.
    
    This is more memory-efficient than loading all IDs upfront for large datasets.
    """
    if not identifiers:
        return set()
    
    query = f"""
    UNWIND $ids as id
    OPTIONAL MATCH (n:{label} {{{identifier_prop}: id}})
    WITH id, n
    WHERE n IS NOT NULL
    RETURN id as identifier
    """
    result = conn.execute_with_retry(query, {"ids": identifiers})
    return {record["identifier"] for record in result if record["identifier"] is not None}


def filter_new_nodes(
    nodes: List[Dict[str, Any]],
    existing_identifiers: set,
    identifier_prop: str,
) -> List[Dict[str, Any]]:
    """Filter nodes to only include those not present in target."""
    new_nodes = []
    for node in nodes:
        props = node.get("props", {})
        identifier = props.get(identifier_prop)
        if identifier is not None and identifier not in existing_identifiers:
            new_nodes.append(node)
    return new_nodes


def filter_new_nodes_via_db(
    target_conn: Neo4jConnection,
    label: str,
    identifier_prop: str,
    nodes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Filter nodes by checking existence in target database.
    
    More efficient than loading all IDs for very large databases.
    Returns (new_nodes, skipped_count).
    """
    if not nodes:
        return [], 0
    
    # Extract identifiers from nodes
    identifiers = []
    id_to_node = {}
    for node in nodes:
        props = node.get("props", {})
        identifier = props.get(identifier_prop)
        if identifier is not None:
            identifiers.append(identifier)
            id_to_node[identifier] = node
    
    # Check which already exist
    existing = check_nodes_exist_batch(target_conn, label, identifier_prop, identifiers)
    
    # Filter to new nodes only
    new_nodes = [
        id_to_node[id] for id in identifiers 
        if id not in existing
    ]
    skipped = len(identifiers) - len(new_nodes)
    
    return new_nodes, skipped


def create_nodes_batch(
    conn: Neo4jConnection,
    label: str,
    nodes: List[Dict[str, Any]],
    dry_run: bool = False,
) -> int:
    """Create a batch of nodes on the target database."""
    if not nodes:
        return 0

    if dry_run:
        logger.debug(f"[DRY RUN] Would create {len(nodes)} nodes with label {label}")
        return len(nodes)

    # Use UNWIND for efficient batch creation
    query = f"""
    UNWIND $nodes as node
    CREATE (n:{label})
    SET n = node.props, n._source_id = node._id
    RETURN count(n) as created
    """

    result = conn.execute_with_retry(query, {"nodes": nodes}, write=True)
    return result[0]["created"] if result else 0


def merge_nodes_batch(
    conn: Neo4jConnection,
    label: str,
    nodes: List[Dict[str, Any]],
    identifier_prop: str,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Merge nodes - create if not exists, skip if exists.
    Returns (created_count, skipped_count).
    """
    if not nodes:
        return 0, 0

    if dry_run:
        logger.debug(
            f"[DRY RUN] Would merge {len(nodes)} nodes with label {label} "
            f"using {identifier_prop} as identifier"
        )
        return len(nodes), 0

    # Use MERGE to only create nodes that don't exist
    query = f"""
    UNWIND $nodes as node
    MERGE (n:{label} {{{identifier_prop}: node.props.{identifier_prop}}})
    ON CREATE SET n = node.props, n._source_id = node._id, n._was_created = true
    ON MATCH SET n._was_created = false
    RETURN 
        sum(CASE WHEN n._was_created THEN 1 ELSE 0 END) as created,
        sum(CASE WHEN n._was_created THEN 0 ELSE 1 END) as skipped
    """

    result = conn.execute_with_retry(query, {"nodes": nodes}, write=True)
    
    if result:
        created = result[0].get("created", 0)
        skipped = result[0].get("skipped", 0)
        return created, skipped
    return 0, 0


def cleanup_merge_flags(conn: Neo4jConnection, dry_run: bool = False) -> None:
    """Remove temporary _was_created flags after merge."""
    if dry_run:
        return

    query = """
    MATCH (n)
    WHERE n._was_created IS NOT NULL
    REMOVE n._was_created
    """
    conn.execute_with_retry(query, write=True)


# =============================================================================
# Relationship Migration Functions
# =============================================================================


def get_relationships_batch(
    conn: Neo4jConnection,
    rel_type: str,
    skip: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """Get a batch of relationships with their properties."""
    query = f"""
    MATCH (a)-[r:{rel_type}]->(b)
    RETURN id(a) as start_id, id(b) as end_id, 
           properties(r) as props, labels(a)[0] as start_label, 
           labels(b)[0] as end_label
    ORDER BY id(r)
    SKIP $skip
    LIMIT $limit
    """
    return conn.execute_with_retry(query, {"skip": skip, "limit": limit})


def create_relationships_batch(
    conn: Neo4jConnection,
    rel_type: str,
    relationships: List[Dict[str, Any]],
    dry_run: bool = False,
) -> int:
    """Create a batch of relationships on the target database."""
    if not relationships:
        return 0

    if dry_run:
        logger.debug(
            f"[DRY RUN] Would create {len(relationships)} "
            f"relationships of type {rel_type}"
        )
        return len(relationships)

    # Create relationships by matching on _source_id
    query = f"""
    UNWIND $rels as rel
    MATCH (a {{_source_id: rel.start_id}})
    MATCH (b {{_source_id: rel.end_id}})
    CREATE (a)-[r:{rel_type}]->(b)
    SET r = rel.props
    RETURN count(r) as created
    """

    result = conn.execute_with_retry(query, {"rels": relationships}, write=True)
    return result[0]["created"] if result else 0


def cleanup_source_ids(conn: Neo4jConnection, dry_run: bool = False) -> None:
    """Remove temporary _source_id properties after migration."""
    if dry_run:
        logger.info("[DRY RUN] Would remove _source_id properties")
        return

    query = """
    MATCH (n)
    WHERE n._source_id IS NOT NULL
    REMOVE n._source_id
    RETURN count(n) as cleaned
    """

    result = conn.execute_with_retry(query, write=True)
    count = result[0]["cleaned"] if result else 0
    logger.info(f"Cleaned up {count} _source_id properties")
