#!/usr/bin/env python3
"""
Neo4j 4.4 CE Data Migration Script

Migrates data from a source Neo4j 4.4 CE instance to a target Neo4j 4.4 CE instance.
Supports live migration without downtime using batched operations.

Usage:
    python migrate.py [options]

Options:
    --dry-run       Run without making changes to target
    --incremental   Only migrate nodes/labels not present in target (default mode)
    --full          Migrate all data (may create duplicates)
    --skip-schema   Skip schema (indexes/constraints) migration
    --skip-nodes    Skip node migration
    --skip-rels     Skip relationship migration
    --skip-cleanup  Skip cleanup of temporary _source_id properties
    --verbose       Enable verbose output
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from tqdm import tqdm

import config
from utils.neo4j_utils import (
    Neo4jConnection,
    MigrationError,
    get_all_labels,
    get_all_relationship_types,
    get_relationship_types_for_labels,
    get_indexes,
    get_constraints,
    create_index,
    create_constraint,
    get_node_count,
    get_relationship_count,
    get_nodes_batch,
    get_node_identifier_property,
    get_existing_node_identifiers,
    filter_new_nodes,
    create_nodes_batch,
    merge_nodes_batch,
    cleanup_merge_flags,
    get_relationships_batch,
    create_relationships_batch,
    cleanup_source_ids,
    get_database_stats,
)
from utils.checkpoint import CheckpointManager

# Configure logging
def setup_logging(verbose: bool = False, log_file: Optional[str] = None):
    """Configure logging for the migration script."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


logger = logging.getLogger(__name__)


class MigrationProgress:
    """Track migration progress and statistics."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.nodes_migrated = 0
        self.nodes_skipped = 0
        self.relationships_migrated = 0
        self.relationships_skipped = 0
        self.errors = []
        self.warnings = []
    
    def start(self):
        self.start_time = datetime.now()
        logger.info(f"Migration started at {self.start_time}")
    
    def finish(self):
        self.end_time = datetime.now()
        duration = self.end_time - self.start_time
        logger.info(f"Migration completed at {self.end_time}")
        logger.info(f"Total duration: {duration}")
    
    def add_error(self, error: str):
        self.errors.append(error)
        logger.error(error)
    
    def add_warning(self, warning: str):
        self.warnings.append(warning)
        logger.warning(warning)
    
    def summary(self) -> str:
        """Generate a summary of the migration."""
        lines = [
            "\n" + "=" * 60,
            "MIGRATION SUMMARY",
            "=" * 60,
            f"Start Time: {self.start_time}",
            f"End Time: {self.end_time}",
            f"Duration: {self.end_time - self.start_time if self.end_time else 'N/A'}",
            f"Nodes Migrated: {self.nodes_migrated}",
            f"Nodes Skipped (already exist): {self.nodes_skipped}",
            f"Relationships Migrated: {self.relationships_migrated}",
            f"Relationships Skipped: {self.relationships_skipped}",
            f"Errors: {len(self.errors)}",
            f"Warnings: {len(self.warnings)}",
            "=" * 60,
        ]
        
        if self.errors:
            lines.append("\nERRORS:")
            for error in self.errors:
                lines.append(f"  - {error}")
        
        if self.warnings:
            lines.append("\nWARNINGS:")
            for warning in self.warnings:
                lines.append(f"  - {warning}")
        
        return "\n".join(lines)


def migrate_schema(
    source: Neo4jConnection,
    target: Neo4jConnection,
    dry_run: bool = False,
    labels_filter: Optional[List[str]] = None,
) -> None:
    """Migrate indexes and constraints from source to target.

    If labels_filter is provided, only indexes/constraints for those labels
    are migrated.
    """
    logger.info("Migrating schema (indexes and constraints)...")

    filter_set = set(labels_filter) if labels_filter else None

    indexes = get_indexes(source)
    for index in indexes:
        if index.get("type") == "LOOKUP":
            continue

        index_labels = index.get("labelsOrTypes", [])
        properties = index.get("properties", [])

        if index_labels and properties:
            if filter_set and index_labels[0] not in filter_set:
                continue
            try:
                create_index(
                    target,
                    index_labels[0],
                    properties,
                    dry_run=dry_run,
                )
            except Exception as e:
                logger.warning(f"Failed to create index {index.get('name')}: {e}")

    constraints = get_constraints(source)
    for constraint in constraints:
        constraint_labels = constraint.get("labelsOrTypes", [])
        properties = constraint.get("properties", [])
        constraint_type = constraint.get("type", "")

        if constraint_labels and properties and "UNIQUE" in constraint_type.upper():
            if filter_set and constraint_labels[0] not in filter_set:
                continue
            try:
                create_constraint(
                    target,
                    constraint_labels[0],
                    properties,
                    "UNIQUENESS",
                    dry_run=dry_run,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to create constraint {constraint.get('name')}: {e}"
                )


def migrate_nodes(
    source: Neo4jConnection,
    target: Neo4jConnection,
    labels: List[str],
    batch_size: int,
    progress: MigrationProgress,
    dry_run: bool = False,
    incremental: bool = True,
    checkpoint: Optional['CheckpointManager'] = None,
) -> None:
    """
    Migrate nodes from source to target.
    
    Optimized for large-scale migrations (100M+ nodes):
    - Uses database-side filtering instead of loading all IDs into memory
    - Supports checkpointing for resumability
    - Shows ETA based on processing rate
    
    Args:
        incremental: If True (default), only migrate nodes not present in target.
        checkpoint: Optional checkpoint manager for resumability.
    """
    from utils.neo4j_utils import (
        get_existing_node_count,
        filter_new_nodes_via_db,
    )
    
    for label in labels:
        # Check if already completed (for resume)
        if checkpoint and checkpoint.is_label_complete(label):
            logger.info(f"Label '{label}' already migrated, skipping.")
            continue
        
        total_count = get_node_count(source, label)
        
        if total_count == 0:
            logger.info(f"No nodes found with label '{label}', skipping.")
            if checkpoint:
                checkpoint.mark_label_complete(label)
            continue
        
        # For incremental mode, find the identifier property
        identifier_prop = None
        if incremental:
            identifier_prop = get_node_identifier_property(source, label)
            if identifier_prop:
                logger.debug(
                    f"Using '{identifier_prop}' as identifier for label '{label}'"
                )
                target_count = get_existing_node_count(target, label)
                logger.info(
                    f"[{label}] Source: {total_count:,} nodes, Target: {target_count:,} nodes"
                )
            else:
                progress.add_warning(
                    f"No unique identifier found for '{label}'. "
                    f"Using full migration for this label (may create duplicates)."
                )
        
        # Get resume offset from checkpoint
        start_offset = 0
        if checkpoint:
            start_offset = checkpoint.get_label_offset(label)
            if start_offset > 0:
                logger.info(f"Resuming '{label}' from offset {start_offset:,}")
        
        logger.debug(f"Processing {total_count:,} nodes with label '{label}'...")
        
        with tqdm(
            total=total_count,
            initial=start_offset,
            desc=f"Migrating {label}",
            unit="nodes",
            smoothing=0.1,  # More stable ETA for large datasets
        ) as pbar:
            skip = start_offset
            batch_count = 0
            
            while skip < total_count:
                try:
                    # Get batch from source
                    nodes = get_nodes_batch(source, label, skip, batch_size)
                    
                    if not nodes:
                        break
                    
                    batch_migrated = 0
                    batch_skipped = 0
                    
                    if incremental and identifier_prop:
                        # Use database-side filtering (no memory overhead)
                        new_nodes, skipped = filter_new_nodes_via_db(
                            target, label, identifier_prop, nodes
                        )
                        batch_skipped = skipped
                        progress.nodes_skipped += skipped
                        
                        if new_nodes:
                            # Create only new nodes
                            created = create_nodes_batch(target, label, new_nodes, dry_run)
                            batch_migrated = created
                            progress.nodes_migrated += created
                    else:
                        # Full migration - create all nodes
                        created = create_nodes_batch(target, label, nodes, dry_run)
                        batch_migrated = created
                        progress.nodes_migrated += created
                    
                    pbar.update(len(nodes))
                    skip += batch_size
                    batch_count += 1
                    
                    # Update checkpoint periodically (every 10 batches)
                    if checkpoint and batch_count % 10 == 0:
                        checkpoint.update_label_progress(
                            label, skip, batch_migrated, batch_skipped
                        )
                    
                except MigrationError as e:
                    progress.add_error(f"Error migrating {label} batch at {skip}: {e}")
                    skip += batch_size  # Skip failed batch and continue
        
        # Mark label complete
        if checkpoint:
            checkpoint.mark_label_complete(label)
    
    logger.info(
        f"Nodes — migrated: {progress.nodes_migrated:,}, skipped: {progress.nodes_skipped:,}"
    )


def migrate_relationships(
    source: Neo4jConnection,
    target: Neo4jConnection,
    rel_types: List[str],
    batch_size: int,
    progress: MigrationProgress,
    dry_run: bool = False,
) -> None:
    """Migrate all relationships from source to target."""
    for rel_type in rel_types:
        total_count = get_relationship_count(source, rel_type)

        if total_count == 0:
            logger.debug(f"No relationships of type '{rel_type}', skipping.")
            continue
        
        with tqdm(
            total=total_count, desc=f"Migrating {rel_type}", unit="rels"
        ) as pbar:
            skip = 0
            while skip < total_count:
                try:
                    # Get batch from source
                    rels = get_relationships_batch(source, rel_type, skip, batch_size)
                    
                    if not rels:
                        break
                    
                    # Create batch on target
                    created = create_relationships_batch(target, rel_type, rels, dry_run)
                    progress.relationships_migrated += created
                    
                    pbar.update(len(rels))
                    skip += batch_size
                    
                except MigrationError as e:
                    progress.add_error(
                        f"Error migrating {rel_type} batch at {skip}: {e}"
                    )
                    skip += batch_size  # Skip failed batch and continue
    
    logger.info(f"Relationships — migrated: {progress.relationships_migrated:,}")


def verify_migration(
    source: Neo4jConnection,
    target: Neo4jConnection,
    progress: MigrationProgress,
    labels: Optional[List[str]] = None,
    rel_types: Optional[List[str]] = None,
) -> bool:
    """Verify that the migration was successful."""
    logger.info("Verifying migration...")

    source_stats = get_database_stats(source, labels=labels, rel_types=rel_types)
    target_stats = get_database_stats(target, labels=labels, rel_types=rel_types)
    
    success = True
    
    # Compare node counts
    for label, source_count in source_stats["labels"].items():
        target_count = target_stats["labels"].get(label, 0)
        if source_count != target_count:
            progress.add_warning(
                f"Node count mismatch for '{label}': "
                f"source={source_count}, target={target_count}"
            )
            success = False
        else:
            logger.info(f"✓ Label '{label}': {source_count} nodes verified")
    
    # Compare relationship counts
    for rel_type, source_count in source_stats["relationship_types"].items():
        target_count = target_stats["relationship_types"].get(rel_type, 0)
        if source_count != target_count:
            progress.add_warning(
                f"Relationship count mismatch for '{rel_type}': "
                f"source={source_count}, target={target_count}"
            )
            success = False
        else:
            logger.info(f"✓ Relationship '{rel_type}': {source_count} verified")
    
    return success


def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate data between Neo4j 4.4 CE instances"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without making changes to target",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip schema (indexes/constraints) migration",
    )
    parser.add_argument(
        "--skip-nodes",
        action="store_true",
        help="Skip node migration",
    )
    parser.add_argument(
        "--skip-rels",
        action="store_true",
        help="Skip relationship migration",
    )
    parser.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Skip cleanup of temporary _source_id properties",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip verification step",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=config.BATCH_SIZE,
        help=f"Batch size for migration (default: {config.BATCH_SIZE})",
    )
    
    # Migration mode group - mutually exclusive
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Only migrate nodes not present in target (default mode)",
    )
    mode_group.add_argument(
        "--full",
        action="store_true",
        help="Migrate all data (may create duplicates if target has existing data)",
    )
    
    parser.add_argument(
        "--identifier-property",
        type=str,
        default=None,
        help="Property to use as unique identifier for incremental mode (auto-detected if not specified)",
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous checkpoint (default: True if checkpoint exists)",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpointing (not recommended for large migrations)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoint and start fresh",
    )
    
    args = parser.parse_args()
    
    # Determine incremental mode
    incremental = not args.full
    
    # Setup logging
    setup_logging(
        verbose=args.verbose or config.VERBOSE,
        log_file=config.LOG_FILE,
    )
    
    # Determine dry-run mode
    dry_run = args.dry_run or config.DRY_RUN
    
    if dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN MODE - No changes will be made to target database")
        logger.info("=" * 60)
    
    if incremental:
        logger.info("=" * 60)
        logger.info("INCREMENTAL MODE - Only migrating nodes not present in target")
        logger.info("=" * 60)
    
    progress = MigrationProgress()
    
    # Setup checkpointing
    checkpoint = None
    if not args.no_checkpoint and not dry_run:
        checkpoint = CheckpointManager()
        if args.reset:
            logger.info("Resetting checkpoint...")
            checkpoint.clear()
        checkpoint.start_migration()
        logger.info(checkpoint.get_summary())
    
    try:
        # Connect to source and target
        logger.info("Connecting to source database...")
        source = Neo4jConnection(
            uri=config.SOURCE_URI,
            user=config.SOURCE_USER,
            password=config.SOURCE_PASSWORD,
            database=config.SOURCE_DATABASE,
            timeout=config.CONNECTION_TIMEOUT,
            max_retries=config.MAX_RETRIES,
        )
        source.connect()
        
        logger.info("Connecting to target database...")
        target = Neo4jConnection(
            uri=config.TARGET_URI,
            user=config.TARGET_USER,
            password=config.TARGET_PASSWORD,
            database=config.TARGET_DATABASE,
            timeout=config.CONNECTION_TIMEOUT,
            max_retries=config.MAX_RETRIES,
        )
        target.connect()
        
        progress.start()
        
        # Get labels and relationship types to migrate
        labels = config.LABELS_TO_MIGRATE or get_all_labels(source)
        if config.RELATIONSHIP_TYPES_TO_MIGRATE:
            rel_types = config.RELATIONSHIP_TYPES_TO_MIGRATE
        elif config.LABELS_TO_MIGRATE:
            rel_types = get_relationship_types_for_labels(source, labels)
        else:
            rel_types = get_all_relationship_types(source)
        
        logger.debug(f"Labels to migrate: {labels}")
        logger.debug(f"Relationship types to migrate: {rel_types}")

        source_stats = get_database_stats(source, labels=labels, rel_types=rel_types)
        logger.info(
            f"Source DB — nodes: {source_stats['total_nodes']:,}, "
            f"relationships: {source_stats['total_relationships']:,}"
        )
        
        # Phase 1: Schema Migration
        if not args.skip_schema:
            migrate_schema(source, target, dry_run, labels_filter=labels)
        else:
            logger.info("Skipping schema migration (--skip-schema)")
        
        # Phase 2: Node Migration
        if not args.skip_nodes:
            migrate_nodes(
                source, target, labels, args.batch_size, progress, dry_run,
                incremental=incremental,
                checkpoint=checkpoint,
            )
        else:
            logger.info("Skipping node migration (--skip-nodes)")
        
        # Phase 3: Relationship Migration
        if not args.skip_rels:
            migrate_relationships(
                source, target, rel_types, args.batch_size, progress, dry_run
            )
        else:
            logger.info("Skipping relationship migration (--skip-rels)")
        
        # Cleanup temporary _source_id properties
        if not args.skip_cleanup and not args.skip_nodes:
            logger.info("Cleaning up temporary properties...")
            cleanup_source_ids(target, dry_run)
        
        # Verification
        if not args.skip_verify and not dry_run:
            verify_migration(source, target, progress, labels=labels, rel_types=rel_types)
        
        progress.finish()
        
        # Print summary
        print(progress.summary())
        
    except MigrationError as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Migration interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Close connections
        if "source" in locals():
            source.close()
        if "target" in locals():
            target.close()


if __name__ == "__main__":
    main()
