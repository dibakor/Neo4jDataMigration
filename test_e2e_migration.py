"""
End-to-End Migration Test (no Docker required)

Uses an in-memory graph store that intercepts Neo4jConnection.execute_with_retry
so the full migration pipeline (utility functions included) runs against a fake
but faithful backing store.

Run:
    pytest test_e2e_migration.py -v
"""

import re
import shutil
import tempfile
from copy import deepcopy
from unittest.mock import patch

import pytest

from utils.neo4j_utils import Neo4jConnection
from utils.checkpoint import CheckpointManager
from migrate import (
    migrate_schema,
    migrate_nodes,
    migrate_relationships,
    verify_migration,
    MigrationProgress,
)
from utils.neo4j_utils import (
    get_all_labels,
    get_all_relationship_types,
    get_node_count,
    get_relationship_count,
    get_database_stats,
    cleanup_source_ids,
)


# ---------------------------------------------------------------------------
# In-memory graph store
# ---------------------------------------------------------------------------

class InMemoryGraph:
    """Minimal in-memory graph that responds to the Cypher patterns used by
    the migration code.  Each instance acts as one 'database'."""

    def __init__(self):
        self._next_id = 1
        # {node_id: {"labels": set, "props": dict}}
        self.nodes = {}
        # [(start_id, end_id, rel_type, props)]
        self.rels = []
        # [{"name": ..., "type": ..., "labelsOrTypes": [...], "properties": [...]}]
        self.indexes = []
        self.constraints = []

    # -- helpers --------------------------------------------------------

    def _add_node(self, labels, props):
        nid = self._next_id
        self._next_id += 1
        self.nodes[nid] = {"labels": set(labels), "props": dict(props)}
        return nid

    def _labels(self):
        out = set()
        for n in self.nodes.values():
            out.update(n["labels"])
        return sorted(out)

    def _rel_types(self):
        return sorted({r[2] for r in self.rels})

    # -- query dispatcher -----------------------------------------------

    def execute(self, query, params=None, write=False):
        q = " ".join(query.split())  # normalise whitespace
        params = params or {}
        return self._dispatch(q, params, write)

    def _dispatch(self, q, params, write):
        # db.labels()
        if "db.labels()" in q:
            return [{"label": lb} for lb in self._labels()]

        # db.relationshipTypes()
        if "db.relationshipTypes()" in q:
            return [{"relationshipType": rt} for rt in self._rel_types()]

        # SHOW INDEXES
        if "SHOW INDEXES" in q.upper():
            return deepcopy(self.indexes)

        # SHOW CONSTRAINTS
        if "SHOW CONSTRAINTS" in q.upper():
            return deepcopy(self.constraints)

        # CREATE INDEX
        m = re.search(r"CREATE INDEX (\S+) IF NOT EXISTS FOR \(n:(\w+)\) ON \(([^)]+)\)", q)
        if m:
            name, label, props_str = m.group(1), m.group(2), m.group(3)
            props = [p.strip().replace("n.", "") for p in props_str.split(",")]
            if not any(i["name"] == name for i in self.indexes):
                self.indexes.append({
                    "name": name, "type": "BTREE",
                    "labelsOrTypes": [label], "properties": props,
                })
            return []

        # CREATE CONSTRAINT
        m = re.search(
            r"CREATE CONSTRAINT (\S+) IF NOT EXISTS FOR \(n:(\w+)\) REQUIRE \(([^)]+)\) IS UNIQUE", q
        )
        if m:
            name, label, props_str = m.group(1), m.group(2), m.group(3)
            props = [p.strip().replace("n.", "") for p in props_str.split(",")]
            if not any(c["name"] == name for c in self.constraints):
                self.constraints.append({
                    "name": name, "type": "UNIQUENESS",
                    "labelsOrTypes": [label], "properties": props,
                })
            return []

        # MATCH (n:{Label}) RETURN count(n) as count
        m = re.search(r"MATCH \(n:(\w+)\) RETURN count\(n\) as count", q)
        if m:
            label = m.group(1)
            cnt = sum(1 for n in self.nodes.values() if label in n["labels"])
            return [{"count": cnt}]

        # MATCH (n) RETURN count(n) as count
        if re.search(r"MATCH \(n\) RETURN count\(n\) as count", q):
            return [{"count": len(self.nodes)}]

        # MATCH ()-[r:{TYPE}]->() RETURN count(r) as count
        m = re.search(r"MATCH \(\)-\[r:(\w+)\]->\(\) RETURN count\(r\) as count", q)
        if m:
            rt = m.group(1)
            cnt = sum(1 for r in self.rels if r[2] == rt)
            return [{"count": cnt}]

        # MATCH ()-[r]->() RETURN count(r) as count
        if re.search(r"MATCH \(\)-\[r\]->\(\) RETURN count\(r\) as count", q):
            return [{"count": len(self.rels)}]

        # keys(n) LIMIT 1 — sample a node for identifier detection
        m = re.search(r"MATCH \(n:(\w+)\) RETURN keys\(n\) as props LIMIT 1", q)
        if m:
            label = m.group(1)
            for n in self.nodes.values():
                if label in n["labels"]:
                    return [{"props": list(n["props"].keys())}]
            return []

        # get_nodes_batch: MATCH (n:{label}) … SKIP/LIMIT
        m = re.search(
            r"MATCH \(n:(\w+)\) RETURN id\(n\) as _id, properties\(n\) as props "
            r"ORDER BY id\(n\) SKIP \$skip LIMIT \$limit", q
        )
        if m:
            label = m.group(1)
            matching = sorted(
                [(nid, n) for nid, n in self.nodes.items() if label in n["labels"]],
                key=lambda x: x[0],
            )
            skip, limit = params.get("skip", 0), params.get("limit", 100)
            batch = matching[skip: skip + limit]
            return [{"_id": nid, "props": dict(n["props"])} for nid, n in batch]

        # create_nodes_batch: UNWIND $nodes … CREATE (n:{label})
        m = re.search(
            r"UNWIND \$nodes as node CREATE \(n:(\w+)\) "
            r"SET n = node\.props, n\._source_id = node\._id "
            r"RETURN count\(n\) as created", q
        )
        if m:
            label = m.group(1)
            nodes_param = params.get("nodes", [])
            created = 0
            for nd in nodes_param:
                props = dict(nd.get("props", {}))
                props["_source_id"] = nd["_id"]
                self._add_node([label], props)
                created += 1
            return [{"created": created}]

        # check_nodes_exist_batch: UNWIND $ids … OPTIONAL MATCH
        m = re.search(r"UNWIND \$ids as id OPTIONAL MATCH \(n:(\w+)", q)
        if m:
            label = m.group(1)
            # extract identifier property from pattern {prop: id}
            pm = re.search(r"\{(\w+): id\}", q)
            prop = pm.group(1) if pm else "id"
            ids_param = params.get("ids", [])
            existing = set()
            for n in self.nodes.values():
                if label in n["labels"] and n["props"].get(prop) in ids_param:
                    existing.add(n["props"][prop])
            return [{"identifier": eid} for eid in existing]

        # get_relationships_batch
        m = re.search(
            r"MATCH \(a\)-\[r:(\w+)\]->\(b\) RETURN id\(a\) as start_id, id\(b\) as end_id",
            q,
        )
        if m:
            rt = m.group(1)
            matching = [(s, e, t, p) for s, e, t, p in self.rels if t == rt]
            skip, limit = params.get("skip", 0), params.get("limit", 100)
            batch = matching[skip: skip + limit]
            results = []
            for s, e, t, p in batch:
                s_labels = self.nodes.get(s, {}).get("labels", set())
                e_labels = self.nodes.get(e, {}).get("labels", set())
                results.append({
                    "start_id": s,
                    "end_id": e,
                    "props": dict(p),
                    "start_label": next(iter(s_labels), ""),
                    "end_label": next(iter(e_labels), ""),
                })
            return results

        # create_relationships_batch
        m = re.search(
            r"UNWIND \$rels as rel MATCH \(a \{_source_id: rel\.start_id\}\) "
            r"MATCH \(b \{_source_id: rel\.end_id\}\) "
            r"CREATE \(a\)-\[r:(\w+)\]->\(b\)",
            q,
        )
        if m:
            rt = m.group(1)
            rels_param = params.get("rels", [])
            created = 0
            # build _source_id -> node_id lookup
            sid_map = {}
            for nid, n in self.nodes.items():
                sid = n["props"].get("_source_id")
                if sid is not None:
                    sid_map[sid] = nid
            for rel in rels_param:
                a = sid_map.get(rel["start_id"])
                b = sid_map.get(rel["end_id"])
                if a is not None and b is not None:
                    self.rels.append((a, b, rt, dict(rel.get("props", {}))))
                    created += 1
            return [{"created": created}]

        # cleanup_source_ids
        if "_source_id IS NOT NULL" in q and "REMOVE" in q.upper():
            cleaned = 0
            for n in self.nodes.values():
                if "_source_id" in n["props"]:
                    del n["props"]["_source_id"]
                    cleaned += 1
            return [{"cleaned": cleaned}]

        return []


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_conn(graph: InMemoryGraph, name: str) -> Neo4jConnection:
    """Return a Neo4jConnection whose execute_with_retry delegates to *graph*."""
    conn = Neo4jConnection.__new__(Neo4jConnection)
    conn.uri = f"bolt://{name}:7687"
    conn.user = "neo4j"
    conn.password = "test"
    conn.database = "neo4j"
    conn.timeout = 30
    conn.max_retries = 3
    conn._driver = None
    conn._graph = graph  # stash reference
    return conn


def _patched_execute(self, query, parameters=None, write=False):
    return self._graph.execute(query, parameters, write)


def _patched_connect(self):
    return None


def _patched_close(self):
    pass


@pytest.fixture()
def graphs():
    """Return (source_graph, target_graph) pair, freshly created."""
    return InMemoryGraph(), InMemoryGraph()


@pytest.fixture()
def connections(graphs):
    """Return (source_conn, target_conn) wired to the in-memory graphs."""
    src_g, tgt_g = graphs
    src = _make_conn(src_g, "source")
    tgt = _make_conn(tgt_g, "target")
    return src, tgt


@pytest.fixture(autouse=True)
def patch_neo4j():
    """Patch Neo4jConnection so it never touches a real Neo4j server."""
    with patch.object(Neo4jConnection, "execute_with_retry", _patched_execute), \
         patch.object(Neo4jConnection, "connect", _patched_connect), \
         patch.object(Neo4jConnection, "close", _patched_close):
        yield


def _seed(graph: InMemoryGraph):
    """Populate a graph with representative test data."""
    # Constraint + index
    graph.constraints.append({
        "name": "person_id",
        "type": "UNIQUENESS",
        "labelsOrTypes": ["Person"],
        "properties": ["id"],
    })
    graph.indexes.append({
        "name": "product_name",
        "type": "BTREE",
        "labelsOrTypes": ["Product"],
        "properties": ["name"],
    })

    # Persons
    person_ids = {}
    for i in range(1, 51):
        nid = graph._add_node(["Person"], {"id": i, "name": f"Person_{i}", "age": 20 + (i % 40)})
        person_ids[i] = nid

    # Products
    product_ids = {}
    for i in range(1, 31):
        nid = graph._add_node(["Product"], {"id": i, "name": f"Product_{i}", "price": round(i * 9.99, 2)})
        product_ids[i] = nid

    # Orders
    order_ids = {}
    for i in range(1, 21):
        nid = graph._add_node(["Order"], {"order_number": f"ORD-{i}", "total": round(i * 15.5, 2)})
        order_ids[i] = nid

    # PURCHASED: Person -[:PURCHASED]-> Product where person.id % 5 == product.id % 5
    for pid in range(1, 51):
        for prid in range(1, 31):
            if pid % 5 == prid % 5:
                graph.rels.append((
                    person_ids[pid], product_ids[prid], "PURCHASED",
                    {"quantity": pid % 3 + 1, "date": "2025-01-01"},
                ))

    # KNOWS: Person i -> Person i+1
    for i in range(1, 50):
        graph.rels.append((person_ids[i], person_ids[i + 1], "KNOWS", {"since": 2020}))

    # PLACED: Person i -> Order i (for i <= 20)
    for i in range(1, 21):
        graph.rels.append((person_ids[i], order_ids[i], "PLACED", {}))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullMigration:

    def test_full_migration(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()

        labels = get_all_labels(source)
        rel_types = get_all_relationship_types(source)

        migrate_schema(source, target, dry_run=False)
        migrate_nodes(source, target, labels, 10, progress, dry_run=False, incremental=False)
        migrate_relationships(source, target, rel_types, 10, progress, dry_run=False)
        cleanup_source_ids(target)
        progress.finish()

        source_stats = get_database_stats(source)
        target_stats = get_database_stats(target)

        assert target_stats["total_nodes"] == source_stats["total_nodes"]
        assert target_stats["total_relationships"] == source_stats["total_relationships"]

        for label in labels:
            assert get_node_count(target, label) == get_node_count(source, label), \
                f"Node count mismatch for '{label}'"

        for rt in rel_types:
            assert get_relationship_count(target, rt) == get_relationship_count(source, rt), \
                f"Rel count mismatch for '{rt}'"

        assert progress.nodes_migrated == source_stats["total_nodes"]
        assert progress.relationships_migrated == source_stats["total_relationships"]
        assert len(progress.errors) == 0

    def test_node_properties_preserved(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()
        migrate_nodes(source, target, ["Person"], 50, progress, dry_run=False, incremental=False)
        cleanup_source_ids(target)

        # Find Person with id=1 in target graph
        found = [
            n for n in tgt_g.nodes.values()
            if "Person" in n["labels"] and n["props"].get("id") == 1
        ]
        assert len(found) == 1
        assert found[0]["props"]["name"] == "Person_1"
        assert found[0]["props"]["age"] == 21

    def test_relationship_properties_preserved(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()
        migrate_nodes(
            source, target, get_all_labels(source), 100, progress,
            dry_run=False, incremental=False,
        )
        migrate_relationships(source, target, ["KNOWS"], 100, progress, dry_run=False)
        cleanup_source_ids(target)

        knows_rels = [r for r in tgt_g.rels if r[2] == "KNOWS"]
        assert len(knows_rels) > 0
        assert knows_rels[0][3]["since"] == 2020


class TestIncrementalMigration:

    def test_incremental_skips_existing(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        p1 = MigrationProgress()
        p1.start()
        migrate_nodes(source, target, ["Person"], 25, p1, dry_run=False, incremental=True)
        first_count = get_node_count(target, "Person")

        p2 = MigrationProgress()
        p2.start()
        migrate_nodes(source, target, ["Person"], 25, p2, dry_run=False, incremental=True)
        second_count = get_node_count(target, "Person")

        assert first_count == second_count
        assert p2.nodes_skipped == first_count
        assert p2.nodes_migrated == 0

    def test_incremental_picks_up_new_nodes(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        p1 = MigrationProgress()
        p1.start()
        migrate_nodes(source, target, ["Person"], 50, p1, dry_run=False, incremental=True)
        count_after_first = get_node_count(target, "Person")

        # Add 5 new persons to source
        for i in range(51, 56):
            src_g._add_node(["Person"], {"id": i, "name": f"Person_{i}", "age": 30})

        p2 = MigrationProgress()
        p2.start()
        migrate_nodes(source, target, ["Person"], 50, p2, dry_run=False, incremental=True)
        count_after_second = get_node_count(target, "Person")

        assert count_after_second == count_after_first + 5
        assert p2.nodes_migrated == 5


class TestSchemaMigration:

    def test_indexes_migrated(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        migrate_schema(source, target, dry_run=False)

        target_index_labels = {idx["labelsOrTypes"][0] for idx in tgt_g.indexes}
        assert "Product" in target_index_labels

    def test_constraints_migrated(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        migrate_schema(source, target, dry_run=False)

        target_constraint_labels = {c["labelsOrTypes"][0] for c in tgt_g.constraints}
        assert "Person" in target_constraint_labels

    def test_schema_idempotent(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        migrate_schema(source, target, dry_run=False)
        migrate_schema(source, target, dry_run=False)

        # Should not duplicate
        assert len(tgt_g.indexes) == 1
        assert len(tgt_g.constraints) == 1


class TestDryRun:

    def test_dry_run_creates_nothing(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()
        migrate_nodes(
            source, target, get_all_labels(source), 50, progress,
            dry_run=True, incremental=False,
        )

        assert get_node_count(target) == 0
        assert progress.nodes_migrated > 0


class TestCheckpointResume:

    def test_checkpoint_resume(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        tmpdir = tempfile.mkdtemp()
        try:
            cp = CheckpointManager(checkpoint_dir=tmpdir)
            cp.start_migration()

            p1 = MigrationProgress()
            p1.start()
            migrate_nodes(
                source, target, ["Person"], 10, p1,
                dry_run=False, incremental=False, checkpoint=cp,
            )
            assert cp.is_label_complete("Person")
            assert p1.nodes_migrated == 50

            # Simulate resume with a fresh checkpoint manager pointing at same dir
            cp2 = CheckpointManager(checkpoint_dir=tmpdir)
            cp2.start_migration()

            p2 = MigrationProgress()
            p2.start()
            migrate_nodes(
                source, target, ["Person"], 10, p2,
                dry_run=False, incremental=False, checkpoint=cp2,
            )
            assert p2.nodes_migrated == 0
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestVerification:

    def test_verify_passes_after_full_migration(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()
        labels = get_all_labels(source)
        rel_types = get_all_relationship_types(source)

        migrate_nodes(source, target, labels, 100, progress, dry_run=False, incremental=False)
        migrate_relationships(source, target, rel_types, 100, progress, dry_run=False)
        cleanup_source_ids(target)

        ok = verify_migration(source, target, progress)
        assert ok is True
        assert len(progress.warnings) == 0

    def test_verify_detects_mismatch(self, graphs, connections):
        src_g, tgt_g = graphs
        source, target = connections
        _seed(src_g)

        progress = MigrationProgress()
        progress.start()
        migrate_nodes(source, target, ["Person"], 100, progress, dry_run=False, incremental=False)
        cleanup_source_ids(target)

        ok = verify_migration(source, target, progress)
        assert ok is False
        assert len(progress.warnings) > 0
