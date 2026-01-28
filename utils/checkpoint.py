"""
Checkpoint Manager for Large-Scale Neo4j Migrations

Provides resumability for migrations that may take hours or days.
Saves progress to disk so migrations can be resumed after interruption.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manages migration checkpoints for resumability.
    
    Saves progress per label/relationship type so that if migration is
    interrupted, it can resume from the last successful batch.
    """
    
    def __init__(self, checkpoint_dir: str = ".migration_checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "progress.json"
        self._data: Dict[str, Any] = self._load()
    
    def _load(self) -> Dict[str, Any]:
        """Load checkpoint data from disk."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
                    return data
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return {
            "started_at": None,
            "last_updated": None,
            "nodes": {},
            "relationships": {},
            "completed_labels": [],
            "completed_rel_types": [],
            "stats": {
                "total_nodes_migrated": 0,
                "total_nodes_skipped": 0,
                "total_rels_migrated": 0,
                "total_rels_skipped": 0,
            }
        }
    
    def _save(self) -> None:
        """Save checkpoint data to disk."""
        self._data["last_updated"] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump(self._data, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def start_migration(self) -> None:
        """Mark the start of a new migration (or continuation)."""
        if self._data["started_at"] is None:
            self._data["started_at"] = datetime.now().isoformat()
            self._save()
            logger.info("Started new migration")
        else:
            logger.info(f"Resuming migration started at {self._data['started_at']}")
    
    def get_label_offset(self, label: str) -> int:
        """Get the current offset for a label (for resuming)."""
        return self._data["nodes"].get(label, {}).get("offset", 0)
    
    def update_label_progress(
        self,
        label: str,
        offset: int,
        migrated: int,
        skipped: int,
    ) -> None:
        """Update progress for a label."""
        if label not in self._data["nodes"]:
            self._data["nodes"][label] = {
                "offset": 0,
                "migrated": 0,
                "skipped": 0,
            }
        
        self._data["nodes"][label]["offset"] = offset
        self._data["nodes"][label]["migrated"] += migrated
        self._data["nodes"][label]["skipped"] += skipped
        self._data["stats"]["total_nodes_migrated"] += migrated
        self._data["stats"]["total_nodes_skipped"] += skipped
        self._save()
    
    def mark_label_complete(self, label: str) -> None:
        """Mark a label as fully migrated."""
        if label not in self._data["completed_labels"]:
            self._data["completed_labels"].append(label)
            self._save()
            logger.info(f"Completed migration for label: {label}")
    
    def is_label_complete(self, label: str) -> bool:
        """Check if a label has been fully migrated."""
        return label in self._data["completed_labels"]
    
    def get_rel_type_offset(self, rel_type: str) -> int:
        """Get the current offset for a relationship type."""
        return self._data["relationships"].get(rel_type, {}).get("offset", 0)
    
    def update_rel_type_progress(
        self,
        rel_type: str,
        offset: int,
        migrated: int,
        skipped: int = 0,
    ) -> None:
        """Update progress for a relationship type."""
        if rel_type not in self._data["relationships"]:
            self._data["relationships"][rel_type] = {
                "offset": 0,
                "migrated": 0,
                "skipped": 0,
            }
        
        self._data["relationships"][rel_type]["offset"] = offset
        self._data["relationships"][rel_type]["migrated"] += migrated
        self._data["relationships"][rel_type]["skipped"] += skipped
        self._data["stats"]["total_rels_migrated"] += migrated
        self._data["stats"]["total_rels_skipped"] += skipped
        self._save()
    
    def mark_rel_type_complete(self, rel_type: str) -> None:
        """Mark a relationship type as fully migrated."""
        if rel_type not in self._data["completed_rel_types"]:
            self._data["completed_rel_types"].append(rel_type)
            self._save()
            logger.info(f"Completed migration for relationship type: {rel_type}")
    
    def is_rel_type_complete(self, rel_type: str) -> bool:
        """Check if a relationship type has been fully migrated."""
        return rel_type in self._data["completed_rel_types"]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current migration statistics."""
        return self._data["stats"].copy()
    
    def get_summary(self) -> str:
        """Get a human-readable summary of progress."""
        stats = self._data["stats"]
        labels_done = len(self._data["completed_labels"])
        rels_done = len(self._data["completed_rel_types"])
        
        return (
            f"Migration Progress:\n"
            f"  Started: {self._data['started_at']}\n"
            f"  Last Updated: {self._data['last_updated']}\n"
            f"  Labels Completed: {labels_done}\n"
            f"  Relationship Types Completed: {rels_done}\n"
            f"  Nodes Migrated: {stats['total_nodes_migrated']:,}\n"
            f"  Nodes Skipped: {stats['total_nodes_skipped']:,}\n"
            f"  Relationships Migrated: {stats['total_rels_migrated']:,}\n"
            f"  Relationships Skipped: {stats['total_rels_skipped']:,}"
        )
    
    def clear(self) -> None:
        """Clear all checkpoint data (start fresh)."""
        self._data = {
            "started_at": None,
            "last_updated": None,
            "nodes": {},
            "relationships": {},
            "completed_labels": [],
            "completed_rel_types": [],
            "stats": {
                "total_nodes_migrated": 0,
                "total_nodes_skipped": 0,
                "total_rels_migrated": 0,
                "total_rels_skipped": 0,
            }
        }
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        logger.info("Cleared checkpoint data")
