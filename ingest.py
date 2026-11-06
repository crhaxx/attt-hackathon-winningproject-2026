#!/usr/bin/env python3
"""Context Service - ingest pipeline (incremental, Git-driven).

Turns any engineering repository into one SQLite knowledge base (context.db) and keeps it
up to date commit by commit. Nothing here depends on a particular repository's folder names
or conventions:

  * components are discovered from standard manifests (package.json, pyproject.toml,
    go.mod, pom.xml, Cargo.toml, ...) wherever they are in the tree
  * files are classified by standard file names, extensions and content sniffing
    (e.g. a YAML/JSON file that starts with `openapi:` is an API spec)
  * structured formats are parsed (manifests, OpenAPI, compose, Kubernetes, CODEOWNERS,
    SQL migrations replayed in an in-memory SQLite)
  * code is read by common-framework patterns (fast path) and, when OpenRouter is
    configured, by an LLM pass that works for any language (enrich.py)
  * prose, diagrams and verification are LLM stages (enrich.py)

Every fact carries evidence (file, line, quote), its extractor, a status
(observed / declared / outdated) and a confidence. The source repo is never modified.

Provenance: the database records the repository identity, the commit it represents, the
Git blob id of every stored file, the extraction signature and the completion state of the
deterministic and model stages. A re-run only re-extracts files whose blob (or whose shared
extraction context) changed; cross-source checks are recomputed when any input changed.

Usage:
    python3 ingest.py                        # clone/update the default repo, update context.db
    python3 ingest.py --repo-url <git url>   # any other repository
    python3 ingest.py --repo /path/checkout  # an existing local checkout (read-only, clean)
    python3 ingest.py --no-llm               # deterministic stage only
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath

try:
    import yaml  # PyYAML: compose, Kubernetes, CI and OpenAPI YAML
except ImportError:  # pragma: no cover
    yaml = None

import enrich
import gitsnap
import llm

DEFAULT_REPO_URL = "https://github.com/arkik7/att-hackathon-task"
HERE = Path(__file__).resolve().parent

# Bump when extractor, check, rule or classification code changes: every deterministic
# result is then re-extracted (and model results re-checked) even if HEAD did not move.
EXTRACTION_VERSION = "2026-11-06.3"
SCHEMA_VERSION = 2

# --------------------------------------------------------------------------
# Ontology / storage
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE source (
    path TEXT PRIMARY KEY,
    kind TEXT NOT NULL,          -- see classify(); prose is refined by the LLM (wiki, adr, ...)
    archived INTEGER NOT NULL,   -- 1 if under an archive/legacy/history-like folder
    doc_date TEXT,               -- date stated in the file name/header
    git_date TEXT,               -- last commit touching the file
    bytes INTEGER NOT NULL,
    text TEXT,                   -- NULL for binaries/lockfiles (diagrams: stage-2 JSON)
    doc_status TEXT,             -- stage 2: current | historical | proposal | superseded
    summary TEXT                 -- stage 2: one-line summary
);
CREATE TABLE component (
    id TEXT PRIMARY KEY,         -- comp:<name>, endpoint:..., table:<comp>.<table>, team:..., lib:...
    kind TEXT NOT NULL,          -- service, library, project, endpoint, table, team, library-ext, concept
    name TEXT NOT NULL,
    path TEXT,
    props TEXT                   -- JSON
);
CREATE TABLE evidence (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL REFERENCES source(path),
    line INTEGER,
    quote TEXT NOT NULL,
    extractor TEXT NOT NULL
);
CREATE TABLE relation (
    id INTEGER PRIMARY KEY,
    src TEXT NOT NULL REFERENCES component(id),
    type TEXT NOT NULL,          -- DEPENDS_ON, USES_LIBRARY, CALLS, CONFIGURED_TO_CALL, EXPOSES,
                                 -- DECLARES, OWNED_BY, HAS_TABLE, READS, WRITES,
                                 -- DEPLOY_DEPENDS_ON, MENTIONED_IN, DIAGRAM_EDGE, ...
    dst TEXT NOT NULL,
    status TEXT NOT NULL,        -- observed (code/config), declared (spec/docs), outdated
    confidence REAL NOT NULL,
    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
    props TEXT
);
CREATE TABLE conflict (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,          -- conflict | gap | stale
    check_name TEXT NOT NULL,
    subject TEXT NOT NULL,
    description TEXT NOT NULL,
    confidence REAL NOT NULL,
    evidence_ids TEXT NOT NULL,  -- JSON list
    verdict TEXT,                -- stage 2: confirmed | rejected | llm-found
    rationale TEXT,
    fingerprint TEXT             -- stable identity across runs (type + subject + evidence file/quote)
);
CREATE TABLE claim (             -- stage 2: what a document asserts about a component
    id INTEGER PRIMARY KEY,
    component TEXT NOT NULL,
    topic TEXT,
    text TEXT NOT NULL,
    path TEXT NOT NULL,
    doc_date TEXT,
    doc_status TEXT,
    evidence_id INTEGER NOT NULL REFERENCES evidence(id)
);
CREATE TABLE risk (
    id INTEGER PRIMARY KEY,
    rule TEXT NOT NULL,
    severity TEXT NOT NULL,      -- high | medium | low
    subject TEXT,
    description TEXT NOT NULL,
    guidance TEXT NOT NULL,      -- what generated code must not repeat
    confidence REAL NOT NULL,
    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
    fingerprint TEXT
);
CREATE INDEX relation_src ON relation(src);
CREATE INDEX relation_dst ON relation(dst);
"""

RUN_SCHEMA = """
CREATE TABLE run (               -- one row per ingest that changed the database
    id INTEGER PRIMARY KEY,
    repo TEXT,
    commit_sha TEXT,
    finished TEXT,
    llm INTEGER,                 -- 1 if the model stage was complete after the run
    conflicts INTEGER,
    risks INTEGER,
    new INTEGER,
    resolved INTEGER,
    baseline_run INTEGER         -- run this one was compared with (NULL = no comparable baseline)
);
CREATE TABLE change (            -- what appeared / disappeared compared with the baseline run
    run_id INTEGER NOT NULL,
    item TEXT NOT NULL,          -- conflict | risk
    change TEXT NOT NULL,        -- new | resolved
    fingerprint TEXT NOT NULL,
    summary TEXT NOT NULL,
    ref_id INTEGER               -- conflict/risk id in this run's tables (NULL when resolved)
);
"""

# Schema v2 (provenance + incremental units). Applied on top of SCHEMA for new databases and
# as the migration of unversioned databases, so both end up identical.
MIGRATION_V2 = """
ALTER TABLE source ADD COLUMN blob TEXT;          -- Git blob id the row was read from
ALTER TABLE source ADD COLUMN det_kind TEXT;      -- rule-based classification (kind may be refined by the LLM)
ALTER TABLE evidence ADD COLUMN unit TEXT;        -- extraction unit that produced the row
ALTER TABLE relation ADD COLUMN unit TEXT;
ALTER TABLE conflict ADD COLUMN unit TEXT;
ALTER TABLE conflict ADD COLUMN base_confidence REAL;  -- confidence before verification
ALTER TABLE claim ADD COLUMN unit TEXT;
ALTER TABLE risk ADD COLUMN unit TEXT;
CREATE INDEX evidence_unit ON evidence(unit);
CREATE INDEX relation_unit ON relation(unit);
CREATE INDEX conflict_unit ON conflict(unit);
CREATE INDEX claim_unit ON claim(unit);
CREATE INDEX risk_unit ON risk(unit);
CREATE TABLE extraction_unit (   -- what each unit's stored results were derived from
    unit TEXT PRIMARY KEY,       -- det:<path> | det:* | llm:code:<path> | llm:<path> | llm:diagram-check | llm:verify
    stage TEXT NOT NULL,         -- deterministic | model
    path TEXT,                   -- source file of a file-local unit
    blob TEXT,                   -- Git blob id it was extracted from
    signature TEXT NOT NULL,     -- extraction version / model + prompt configuration
    context TEXT,                -- digest of the shared context it depends on (names, ports, catalog)
    status TEXT NOT NULL,        -- complete | partial
    commit_id TEXT NOT NULL      -- snapshot at which it was (re)extracted
);
CREATE TABLE unit_fact (         -- in-memory extraction context a unit contributes (endpoints, calls,
    unit TEXT NOT NULL,          -- internal deps, diagram edges), so cross-source checks run without
    kind TEXT NOT NULL,          -- re-extracting unchanged files
    data TEXT NOT NULL
);
CREATE INDEX unit_fact_unit ON unit_fact(unit);
CREATE TABLE component_decl (    -- which units declare a component; component = best declaration
    unit TEXT NOT NULL,
    id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT,
    props TEXT,
    PRIMARY KEY (unit, id)
);
CREATE TABLE ingest_meta (key TEXT PRIMARY KEY, value TEXT);  -- repository identity
CREATE TABLE stage_state (       -- active snapshot per pipeline stage
    stage TEXT PRIMARY KEY,      -- deterministic | model
    commit_id TEXT,
    signature TEXT,
    status TEXT NOT NULL,        -- complete | incomplete
    detail TEXT,
    updated_at TEXT
);
"""

FTS_SCHEMA = "CREATE VIRTUAL TABLE source_fts USING fts5(path, kind, text)"

DERIVED = "det:*"                    # cross-source deterministic results
DIAGRAM_CHECK = "llm:diagram-check"  # diagram-vs-code comparison (deterministic, on model output)
VERIFY = "llm:verify"                # model verification verdicts and contradictions
CODE_UNIT = "llm:code:"              # prefix of per-file LLM code-pass units

# natural keys used to recognise an unchanged record when a unit is re-extracted
KEYS = {
    "evidence": ("path", "line", "quote", "extractor"),
    "relation": ("src", "type", "dst", "status", "confidence", "evidence_id", "props"),
    "conflict": ("kind", "check_name", "subject", "description", "base_confidence", "evidence_ids"),
    "claim": ("component", "topic", "text", "path", "doc_date", "doc_status", "evidence_id"),
    "risk": ("rule", "severity", "subject", "description", "guidance", "confidence", "evidence_id"),
}
RECORD_TABLES = ("relation", "claim", "risk", "conflict", "evidence")  # delete order


class IngestError(RuntimeError):
    """User-facing error: printed without a traceback."""


def digest(obj) -> str:
    """Short fingerprint of a JSON-able configuration/context value (not used for file contents)."""
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:20]


def unit_group(unit: str) -> str:
    return "derived" if unit == DERIVED else "file" if unit.startswith("det:") else "model"


def _statements(script: str):
    # executescript() would COMMIT the open transaction, so DDL runs statement by statement
    for stmt in re.sub(r"--[^\n]*", "", script).split(";"):
        if stmt.strip():
            yield stmt


def _tables(db) -> set[str]:
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _columns(db, table: str) -> set[str]:
    return {r[1] for r in db.execute(f"PRAGMA table_info({table})")}


def schema_state(db: sqlite3.Connection) -> str:
    version = db.execute("PRAGMA user_version").fetchone()[0]
    tables = _tables(db)
    if version == SCHEMA_VERSION:
        return "current"
    if version > SCHEMA_VERSION:
        raise IngestError(f"database schema v{version} is newer than this ingest.py (v{SCHEMA_VERSION})")
    if version == 0 and not tables:
        return "empty"
    if version == 0 and {"source", "component", "relation", "evidence"} <= tables:
        return "legacy"
    raise IngestError("database is not a Context Service database (unknown tables); use another --db")


def create_schema(db: sqlite3.Connection):
    for stmt in _statements(SCHEMA + RUN_SCHEMA):
        db.execute(stmt)
    _upgrade_v2(db)


def migrate_legacy(db: sqlite3.Connection):
    """Unversioned (rebuild-per-run) database -> v2. Its rows carry no unit/blob, so they are
    removed and the caller performs a one-time full logical refresh inside the same transaction.
    Ingest history (run/change), when present, is kept."""
    _upgrade_v2(db)
    for table in RECORD_TABLES + ("component",):
        db.execute(f"DELETE FROM {table}")
    db.execute("DELETE FROM source")
    if "source_fts" in _tables(db):
        db.execute("DELETE FROM source_fts")


def _upgrade_v2(db: sqlite3.Connection):
    # both unversioned shapes: the original one, and the one with fingerprints + run history
    for table in ("conflict", "risk"):
        if "fingerprint" not in _columns(db, table):
            db.execute(f"ALTER TABLE {table} ADD COLUMN fingerprint TEXT")
    if "run" not in _tables(db):
        for stmt in _statements(RUN_SCHEMA):
            db.execute(stmt)
    for stmt in _statements(MIGRATION_V2):
        db.execute(stmt)
    if "source_fts" not in _tables(db):
        try:
            db.execute(FTS_SCHEMA)
        except sqlite3.OperationalError:
            pass  # no FTS5 in this SQLite build: query.py falls back to LIKE
    db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


@contextmanager
def transaction(db: sqlite3.Connection):
    """One atomic publication: readers see the previous or the new state, never a mix."""
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK")
        raise
    db.execute("COMMIT")


# --------------------------------------------------------------------------
# Run-to-run diff: what is new / resolved since the previous ingest
# --------------------------------------------------------------------------

def _fp(*parts) -> str:
    return hashlib.sha1(json.dumps(parts, ensure_ascii=False).encode()).hexdigest()[:16]


def _quote_key(q: str) -> str:
    return re.sub(r"\s+", " ", q).strip().lower()


def active_findings(db) -> dict[str, tuple[str, str, int]]:
    """fingerprint -> (item, summary, id) for every active conflict and risk."""
    out = {}
    for cid, fp, kind, check, subject, desc in db.execute(
            "SELECT id, fingerprint, kind, check_name, subject, description FROM conflict "
            "WHERE IFNULL(verdict, '') != 'rejected' AND fingerprint IS NOT NULL"):
        out[fp] = ("conflict", f"[{kind}] {check}: {desc[:220]}", cid)
    for rid, fp, sev, rule, subject, desc in db.execute(
            "SELECT id, fingerprint, severity, rule, subject, description FROM risk WHERE fingerprint IS NOT NULL"):
        out[fp] = ("risk", f"[{sev}] {rule} ({subject}): {desc[:200]}", rid)
    return out


def fingerprint_findings(db):
    """(Re)compute fingerprints; only rows whose fingerprint is missing or different are written."""
    ev = {i: (p, _quote_key(q)) for i, p, q in db.execute("SELECT id, path, quote FROM evidence")}
    for cid, kind, check, subject, evs, old in db.execute(
            "SELECT id, kind, check_name, subject, evidence_ids, fingerprint FROM conflict").fetchall():
        fp = _fp("conflict", kind, check, subject, sorted(ev[e] for e in json.loads(evs) if e in ev))
        if fp != old:
            db.execute("UPDATE conflict SET fingerprint = ? WHERE id = ?", (fp, cid))
    for rid, rule, subject, e, old in db.execute(
            "SELECT id, rule, subject, evidence_id, fingerprint FROM risk").fetchall():
        fp = _fp("risk", rule, subject, ev.get(e))
        if fp != old:
            db.execute("UPDATE risk SET fingerprint = ? WHERE id = ?", (fp, rid))


class Store:
    """Writes knowledge records on behalf of the active extraction unit.

    Re-extracting a unit loads its previous rows as a pool keyed by their natural key:
    an identical record keeps its row id (evidence ids, conflict ids and therefore the
    verifier's cached prompts stay stable) and rows that are not produced again are
    deleted by finish(). Components are declared per unit; the component table holds the
    best declaration of every component that at least one unit still declares.
    With no active unit (dry mode) nothing is written.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.unit: str | None = None
        self.step = 0
        self.fts = bool(db.execute("SELECT 1 FROM sqlite_master WHERE name = 'source_fts'").fetchone())
        self.stats: Counter = Counter()   # (group, added|kept|removed) record counts
        self.mutations = 0                # knowledge rows written or deleted
        self._pools: dict[str, dict] = {}
        self._decls: dict[str, dict] = {}
        self._rows: dict[str, dict] = {}
        self._facts: dict[str, list] = {}
        self.reload_components()

    # -- units ---------------------------------------------------------------
    def reload_components(self):
        self._known = {r[0]: tuple(r[1:]) for r in self.db.execute(
            "SELECT id, kind, name, path, props FROM component")}
        self._components = set(self._known)

    def use(self, unit: str | None):
        """Make `unit` the target of following writes (opening its pool on first use)."""
        self.unit = unit
        if unit is None or unit in self._pools:
            return
        pools = {}
        for table, cols in KEYS.items():
            pool: dict[tuple, list[int]] = {}
            for row in self.db.execute(f"SELECT id, {', '.join(cols)} FROM {table} WHERE unit = ? ORDER BY id", (unit,)):
                pool.setdefault(tuple(row[1:]), []).append(row[0])
            pools[table] = pool
        self._pools[unit] = pools
        self._decls[unit] = {r[0]: tuple(r[1:]) for r in self.db.execute(
            "SELECT id, rank, kind, name, path, props FROM component_decl WHERE unit = ?", (unit,))}
        self._rows[unit] = {}
        self._facts[unit] = []

    def finish(self):
        """Close all opened units: delete their records that were not produced again."""
        for unit, pools in self._pools.items():
            group = unit_group(unit)
            for table in RECORD_TABLES:
                ids = [i for lst in pools[table].values() for i in lst]
                for chunk in range(0, len(ids), 500):
                    part = ids[chunk:chunk + 500]
                    self.db.execute(f"DELETE FROM {table} WHERE id IN ({','.join('?' * len(part))})", part)
                self.stats[group, "removed"] += len(ids)
                self.mutations += len(ids)
            old, new = self._decls[unit], self._rows[unit]
            for cid in old.keys() - new.keys():
                self.db.execute("DELETE FROM component_decl WHERE unit = ? AND id = ?", (unit, cid))
            for cid, row in new.items():
                if old.get(cid) != row:
                    self.db.execute("INSERT OR REPLACE INTO component_decl VALUES (?,?,?,?,?,?,?)", (unit, cid, *row))
            self.db.execute("DELETE FROM unit_fact WHERE unit = ?", (unit,))
            self.db.executemany("INSERT INTO unit_fact (unit, kind, data) VALUES (?,?,?)",
                                [(unit, k, d) for k, d in self._facts[unit]])
        self._pools.clear()
        self._decls.clear()
        self._rows.clear()
        self._facts.clear()
        self.unit = None

    def drop(self, unit: str) -> int:
        """Remove everything a unit produced (deleted file, invalidated or failed unit)."""
        for d in (self._pools, self._decls, self._rows, self._facts):
            d.pop(unit, None)
        if self.unit == unit:
            self.unit = None
        n = sum(self.db.execute(f"DELETE FROM {t} WHERE unit = ?", (unit,)).rowcount for t in RECORD_TABLES)
        for table in ("component_decl", "unit_fact", "extraction_unit"):
            self.db.execute(f"DELETE FROM {table} WHERE unit = ?", (unit,))
        self.stats[unit_group(unit), "removed"] += n
        self.mutations += n
        return n

    def mark(self, unit: str, stage: str, commit: str, signature: str, *, path=None, blob=None,
             context=None, status="complete"):
        self.db.execute("INSERT OR REPLACE INTO extraction_unit VALUES (?,?,?,?,?,?,?,?)",
                        (unit, stage, path, blob, signature, context, status, commit))

    def materialize(self) -> int:
        """Bring the component table in line with the current declarations; returns rows changed."""
        want = {r[0]: tuple(r[1:]) for r in self.db.execute(
            "SELECT id, kind, name, path, props FROM (SELECT *, ROW_NUMBER() OVER "
            "(PARTITION BY id ORDER BY rank, unit) AS rn FROM component_decl) WHERE rn = 1")}
        have = {r[0]: tuple(r[1:]) for r in self.db.execute("SELECT id, kind, name, path, props FROM component")}
        changed = 0
        for cid in have.keys() - want.keys():
            self.db.execute("DELETE FROM component WHERE id = ?", (cid,))
            changed += 1
        for cid, row in want.items():
            if have.get(cid) != row:
                self.db.execute("INSERT OR REPLACE INTO component VALUES (?,?,?,?,?)", (cid, *row))
                changed += 1
        self.mutations += changed
        self.reload_components()
        return changed

    # -- records -------------------------------------------------------------
    def _put(self, table: str, key: tuple, extra: dict | None = None) -> int | None:
        if self.unit is None:
            return None
        group = unit_group(self.unit)
        ids = self._pools[self.unit][table].get(key)
        if ids:
            self.stats[group, "kept"] += 1
            return ids.pop(0)
        extra = extra or {}
        cols = KEYS[table] + tuple(extra) + ("unit",)
        cur = self.db.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                              (*key, *extra.values(), self.unit))
        self.stats[group, "added"] += 1
        self.mutations += 1
        return cur.lastrowid

    def component(self, cid: str, kind: str, name: str, path: str | None = None, **props):
        return self._declare(cid, kind, name, path, json.dumps(props) if props else None)

    def keep(self, cid: str) -> str:
        """Declare an existing component from the active unit, so it lives as long as the unit refers to it."""
        kind, name, path, props = self._known[cid]
        return self._declare(cid, kind, name, path, props)

    def _declare(self, cid, kind, name, path, props) -> str:
        if self.unit is None:
            return cid
        rows = self._rows[self.unit]
        if cid not in rows:
            rank = 0 if self.unit == DERIVED else 100 if self.unit.startswith("llm:") else self.step
            self._set_decl(cid, (rank, kind, name, path, props))
        self._components.add(cid)
        self._known.setdefault(cid, (kind, name, path, props))
        return cid

    def declared(self, cid: str) -> tuple | None:
        """(kind, name, path, props) as declared by the active unit in this run."""
        row = self._rows.get(self.unit, {}).get(cid)
        return row[1:] if row else None

    def redeclare(self, cid: str, kind: str, props: dict | None):
        """Change the active unit's own declaration (e.g. kind decided once the whole corpus is known)."""
        rank, _, name, path, _ = self._rows[self.unit][cid]
        self._set_decl(cid, (rank, kind, name, path, json.dumps(props) if props else None))

    def _set_decl(self, cid, row):
        self._rows[self.unit][cid] = row  # written (if changed) by finish()

    def fact(self, kind: str, data):
        if self.unit is not None:
            self._facts[self.unit].append((kind, json.dumps(data)))

    def evidence(self, path: str, line: int | None, quote: str, extractor: str) -> int:
        return self._put("evidence", (path, line, quote.strip()[:400], extractor))

    def relation(self, src, rtype, dst, status, confidence, ev, **props):
        self._put("relation", (src, rtype, dst, status, confidence, ev, json.dumps(props) if props else None))

    def conflict(self, kind, check, subject, description, confidence, evidence_ids, verdict=None):
        return self._put("conflict", (kind, check, subject, description, confidence,
                                      json.dumps(sorted(set(evidence_ids)))),
                         {"confidence": confidence, "verdict": verdict})

    def claim(self, component, topic, text, path, doc_date, doc_status, ev):
        self._put("claim", (component, topic, text, path, doc_date, doc_status, ev))

    def risk(self, rule, severity, subject, description, guidance, confidence, ev):
        self._put("risk", (rule, severity, subject, description, guidance, confidence, ev))

    # -- sources -------------------------------------------------------------
    def set_text(self, path: str, kind: str, text: str | None):
        """source.text and its full-text index row, kept in step."""
        self.db.execute("UPDATE source SET text = ? WHERE path = ?", (text, path))
        self.set_fts(path, kind, text)

    def set_fts(self, path: str, kind: str, text: str | None):
        if self.fts:
            self.db.execute("DELETE FROM source_fts WHERE path = ?", (path,))
            if text is not None:
                self.db.execute("INSERT INTO source_fts VALUES (?,?,?)", (path, kind, text))


def integrity_problems(db: sqlite3.Connection) -> list[str]:
    """References that would dangle after this update; any hit aborts the transaction."""
    checks = {
        "relation without evidence": "SELECT COUNT(*) FROM relation WHERE evidence_id NOT IN (SELECT id FROM evidence)",
        "claim without evidence": "SELECT COUNT(*) FROM claim WHERE evidence_id NOT IN (SELECT id FROM evidence)",
        "risk without evidence": "SELECT COUNT(*) FROM risk WHERE evidence_id NOT IN (SELECT id FROM evidence)",
        "conflict citing missing evidence": "SELECT COUNT(*) FROM conflict c, json_each(c.evidence_ids) j "
                                            "WHERE j.value NOT IN (SELECT id FROM evidence)",
        "evidence for a file not in the snapshot": "SELECT COUNT(*) FROM evidence WHERE path NOT IN (SELECT path FROM source)",
        "relation from unknown component": "SELECT COUNT(*) FROM relation WHERE src NOT IN (SELECT id FROM component)",
        "relation to unknown component/source": "SELECT COUNT(*) FROM relation WHERE dst NOT IN (SELECT id FROM component) "
                                                "AND NOT (dst LIKE 'source:%' AND substr(dst, 8) IN (SELECT path FROM source))",
        "claim about unknown component": "SELECT COUNT(*) FROM claim WHERE component NOT IN (SELECT id FROM component)",
        "source without blob id": "SELECT COUNT(*) FROM source WHERE blob IS NULL",
    }
    for table in RECORD_TABLES:
        checks[f"{table} without a recorded unit"] = (
            f"SELECT COUNT(*) FROM {table} WHERE unit IS NULL OR unit NOT IN (SELECT unit FROM extraction_unit)")
    if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'source_fts'").fetchone():
        checks["search index out of step with sources"] = (
            "SELECT (SELECT COUNT(*) FROM source_fts) != (SELECT COUNT(*) FROM source WHERE text IS NOT NULL) "
            "OR EXISTS (SELECT 1 FROM source_fts f WHERE f.path NOT IN (SELECT path FROM source WHERE text IS NOT NULL))"
            "OR EXISTS (SELECT 1 FROM source_fts f JOIN source s ON s.path = f.path WHERE f.kind != s.kind)")
    return [f"{name}: {n}" for name, q in checks.items() if (n := db.execute(q).fetchone()[0])]


# --------------------------------------------------------------------------
# Collect + classify (standard names, extensions, content sniffing)
# --------------------------------------------------------------------------

IMAGE_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
BINARY_EXT = IMAGE_EXT | {".ico", ".pdf", ".woff", ".woff2", ".ttf", ".eot", ".zip", ".gz", ".jar",
                          ".class", ".so", ".dll", ".exe", ".mp4", ".mp3", ".wasm"}
LOCKFILES = {"pnpm-lock.yaml", "package-lock.json", "yarn.lock", "poetry.lock", "pipfile.lock",
             "cargo.lock", "go.sum", "composer.lock", "gemfile.lock", "bun.lockb", "uv.lock"}
MANIFESTS = {"package.json", "pyproject.toml", "setup.py", "setup.cfg", "requirements.txt", "pipfile",
             "go.mod", "pom.xml", "build.gradle", "build.gradle.kts", "cargo.toml", "gemfile",
             "composer.json", "mix.exs", "pubspec.yaml"}
WORKSPACE_FILES = {"pnpm-workspace.yaml", "lerna.json", "nx.json", "turbo.json", "settings.gradle",
                   "go.work", "rush.json"}
CODE_EXT = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py", ".java", ".kt", ".kts", ".go", ".rs",
            ".rb", ".php", ".cs", ".scala", ".swift", ".c", ".h", ".cc", ".cpp", ".hpp", ".ex", ".exs",
            ".sh", ".bash", ".ps1", ".vue", ".svelte", ".html", ".css", ".scss", ".dart", ".lua", ".r"}
CONTRACT_EXT = {".proto", ".graphql", ".gql", ".avsc", ".thrift", ".wsdl"}
PROSE_EXT = {".md", ".mdx", ".rst", ".txt", ".adoc", ".org"}
CONFIG_EXT = {".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".properties", ".xml", ".env"}
ARCHIVE_SEGS = {"archive", "archived", "deprecated", "legacy", "old", "history", "attic", "obsolete"}
TEST_SEGS = {"test", "tests", "__tests__", "spec", "specs", "e2e", "integration-tests"}
FIXTURE_SEGS = {"fixtures", "fixture", "seed", "seeds", "testdata", "test-data", "sample-data", "samples",
                "mocks", "__mocks__"}
SCHEMA_SEGS = {"migrations", "migration", "migrate", "schema", "schemas", "ddl"}
AGENT_FILES = {"agents.md", "claude.md", "gemini.md", ".cursorrules", "copilot-instructions.md", "skill.md",
               "skills-lock.json", ".windsurfrules"}
AGENT_SEGS = {".agents", ".claude", ".cursor", ".windsurf", ".codex"}
TOOLING_SEGS = {"scripts", "script", "tools", "bin", "hack", "ci"}
GENERIC_DIRS = {"src", "app", "server", "service", "main", "source", "code", "lib", "api", "backend"}
GENERATED_NAME = re.compile(r"(_pb2(_grpc)?\.pyi?|\.pb\.go|_grpc\.pb\.go|\.pb\.(cc|h)|\.g\.(cs|dart)|\.generated\.\w+|"
                            r"_generated\.\w+|\.designer\.cs|grpc\.(java|kt))$")
GENERATED_MARK = re.compile(r"Code generated .{0,80}DO NOT EDIT|@generated|Generated by the protocol buffer compiler|"
                            r"auto-generated|autogenerated|This file was generated|DO NOT EDIT", re.I)
COMMENT_LINE = re.compile(r"^\s*(#|//|/\*|\*|<!--|--|;|%|rem\s)", re.I)
DATE_RE = re.compile(r"\b(20\d\d-\d\d-\d\d)\b")
SPEC_RE = re.compile(r"^\s*[\"']?(openapi|swagger|asyncapi)[\"']?\s*:", re.M)
K8S_RE = re.compile(r"^apiVersion:\s*\S+", re.M)


@dataclass
class Source:
    path: str
    kind: str
    archived: bool
    size: int
    text: str | None
    doc_date: str | None = None
    git_date: str | None = None
    lines: list[str] = field(default_factory=list)
    blob: str | None = None      # Git blob id at the snapshot commit

    @property
    def segs(self) -> list[str]:
        return [p.lower() for p in self.path.split("/")[:-1]]

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]


def classify(rel: str, text: str | None, size: int) -> str:
    p = PurePosixPath(rel.lower())
    name, ext, segs = p.name, p.suffix, set(p.parts[:-1])
    if name in AGENT_FILES or segs & AGENT_SEGS:
        return "agent_instructions"
    if ext in IMAGE_EXT:
        if size < 4000 or name.startswith(("favicon", "icon", "logo")) or segs & {"icons", "fonts"}:
            return "binary"
        return "diagram"
    if ext in BINARY_EXT:
        return "binary"
    if name in LOCKFILES:
        return "lockfile"
    if name == "codeowners":
        return "ownership"
    if (".github" in segs and "workflows" in segs) or ".circleci" in segs or name in {
            ".gitlab-ci.yml", "jenkinsfile", "azure-pipelines.yml", ".travis.yml", "bitbucket-pipelines.yml"}:
        return "ci"
    if name in MANIFESTS or ext == ".csproj":
        return "dependency_manifest"
    if name in WORKSPACE_FILES:
        return "config"
    if name.startswith("dockerfile") or name.endswith(".dockerfile") or name == "procfile" \
            or re.fullmatch(r"(docker-)?compose(\.[\w-]+)?\.ya?ml", name) or ext in {".tf", ".hcl"} \
            or name in {"chart.yaml", "helmfile.yaml", "skaffold.yaml"}:
        return "deployment"
    if text is not None and ext in {".yaml", ".yml", ".json"}:
        if SPEC_RE.search(text[:3000]):
            return "api_spec"
        if ext != ".json" and K8S_RE.search(text) and re.search(r"^kind:\s*\w+", text, re.M):
            return "deployment"
    if ext in CONTRACT_EXT:
        return "api_contract"
    if ext == ".sql" or (segs & SCHEMA_SEGS and ext in {".sql", ".js", ".ts", ".py", ".rb", ".xml", ".yaml"}):
        return "schema"
    if segs & FIXTURE_SEGS and ext in CONFIG_EXT | {".csv", ".sql"}:
        return "fixture"
    stem = name.rsplit(".", 1)[0]
    if segs & TEST_SEGS or re.search(r"(^test_|_test$|\.test$|\.spec$|_spec$|^test$)", stem):
        return "test" if ext in CODE_EXT else "fixture" if ext in CONFIG_EXT else "test"
    if ext in CODE_EXT:
        if GENERATED_NAME.search(name) or (text and GENERATED_MARK.search(text[:1500])):
            return "generated"  # protoc/openapi-generator output etc.: not evidence of hand-written intent
        return "code"
    if name.startswith(".env") or ext in CONFIG_EXT or name.startswith(("tsconfig", ".eslintrc", ".prettierrc")):
        return "config"
    if ext in PROSE_EXT or name in {"readme", "changelog", "contributing"}:
        if re.match(r"(changelog|changes|history|release)", name):
            return "release_note"
        if name.startswith("license") or name.startswith("copying"):
            return "legal"
        return "doc"  # refined into wiki/adr/meeting_note/incident/... by the LLM
    return "other"


def prepare_repo(url: str, repo: Path | None) -> Path:
    if repo:
        return repo
    slug = re.sub(r"[^\w.-]+", "_", url.rstrip("/").removesuffix(".git").split("://")[-1])
    target = HERE / ".cache" / "sources" / slug
    if (target / ".git").exists():
        subprocess.run(["git", "-C", str(target), "pull", "--ff-only", "-q"], check=False)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", "-q", url, str(target)], check=True)
    return target


def git_dates(repo: Path, head: str) -> dict[str, str]:
    """Last commit date per file (one git call)."""
    out = gitsnap.git(repo, "log", "--format=@%cs", "--name-only", head).decode(errors="replace")
    dates: dict[str, str] = {}
    current = None
    for line in out.splitlines():
        if line.startswith("@"):
            current = line[1:]
        elif line and current and line not in dates:
            dates[line] = current
    return dates


def collect(snap: gitsnap.Snapshot) -> list[Source]:
    """Every tracked regular file at HEAD, read from the Git object store by blob id."""
    dates = git_dates(snap.repo, snap.head)

    def readable(rel: str) -> bool:
        name = rel.rsplit("/", 1)[-1].lower()
        return (PurePosixPath(name).suffix not in BINARY_EXT and name not in LOCKFILES
                and snap.sizes[rel] < 2_000_000)

    blobs = gitsnap.read_blobs(snap.repo, [oid for rel, oid in snap.files.items() if readable(rel)])
    sources = []
    for rel, oid in snap.files.items():
        size = snap.sizes[rel]
        text = None
        if readable(rel):
            try:  # same newline handling as Path.read_text()
                text = blobs[oid].decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
            except UnicodeDecodeError:
                text = None
        kind = classify(rel, text, size)  # content sniffing needs the text first
        if text is None and kind not in {"diagram", "lockfile"}:
            kind = "binary"
        archived = bool({p.lower() for p in rel.split("/")[:-1]} & ARCHIVE_SEGS)
        head = "\n".join((text or "").splitlines()[:8])
        m = DATE_RE.search(rel) or DATE_RE.search(head)
        s = Source(rel, kind, archived, size, text, doc_date=m.group(1) if m else None,
                   git_date=dates.get(rel), blob=oid)
        s.lines = (text or "").splitlines()
        sources.append(s)
    return sources


def line_of(s: Source, needle: str, start: int = 0) -> int | None:
    for i in range(start, len(s.lines)):
        if needle in s.lines[i]:
            return i + 1
    return None


def line_at(s: Source, offset: int) -> int:
    return s.text.count("\n", 0, offset) + 1


# --------------------------------------------------------------------------
# Context: components, aliases, address resolution
# --------------------------------------------------------------------------

@dataclass
class Endpoint:
    service: str  # component id
    method: str   # GET/POST/... or * when unknown
    path: str
    norm: str
    origin: str   # code | spec | spec_history | call | doc
    ev: int


LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "[::1]", "host.docker.internal"}
ADDR_VAR = re.compile(r"(?:_SERVICE)?_(?:BASE_URL|API_URL|URL|URI|HOST|ADDR|ADDRESS|ENDPOINT)$")
ENV_PREFIXES = ("NEXT_PUBLIC_", "VITE_", "REACT_APP_", "EXPO_PUBLIC_", "PUBLIC_", "NUXT_PUBLIC_")


def compact(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def norm_path(path: str) -> str:
    path = path.split("?", 1)[0]
    segs = []
    for seg in path.strip("/").split("/"):
        if not seg:
            continue
        if seg.startswith((":", "{", "<", "$")) or "${" in seg or seg == "{}" or seg == "*":
            segs.append("{}")
        else:
            segs.append(seg)
    return "/" + "/".join(segs)


def path_match(a: str, b: str) -> bool:
    sa, sb = a.strip("/").split("/"), b.strip("/").split("/")
    return len(sa) == len(sb) and all(x == y or "{}" in (x, y) for x, y in zip(sa, sb))


@dataclass
class Ctx:
    store: Store
    sources: dict[str, Source]
    repo_name: str = "repo"
    comp_dirs: dict[str, str] = field(default_factory=dict)       # dir -> component id
    aliases: dict[str, str | None] = field(default_factory=dict)  # alias -> cid (None = ambiguous)
    ports: dict[int, set[str]] = field(default_factory=dict)      # port -> component ids
    archived_comps: set[str] = field(default_factory=set)
    endpoints: list[Endpoint] = field(default_factory=list)
    calls: list[tuple[str, str, int]] = field(default_factory=list)          # caller, callee, ev
    deploy_deps: list[tuple[str, str, int]] = field(default_factory=list)
    tables: dict[str, dict[str, set[str]]] = field(default_factory=dict)     # cid -> table -> cols
    dropped: list[tuple[str, str, str, int]] = field(default_factory=list)   # cid, table, col, ev
    unresolved: list[tuple[str, str]] = field(default_factory=list)  # (file, target hint) nobody matched
    owned: set[str] = field(default_factory=set)                     # components with a CODEOWNERS rule
    depends: set[tuple[str, str]] = field(default_factory=set)       # internal DEPENDS_ON (src, dst)
    regen: set[str] = field(default_factory=set)  # paths whose file-local results are re-extracted
    repo: Path | None = None
    blobs: dict[str, bytes] = field(default_factory=dict)
    _seen: dict = field(default_factory=dict)     # unit -> dedupe keys (file-local, never cross-file)
    _facts: list = field(default_factory=list)    # (order, kind, value, key) from file-local extraction
    _seq: int = 0

    def reset(self):
        """Forget everything derived from the corpus (before a fresh pass over it)."""
        for name in ("comp_dirs", "aliases", "ports", "tables", "_seen"):
            getattr(self, name).clear()
        for name in ("archived_comps", "owned", "depends"):
            getattr(self, name).clear()
        for name in ("endpoints", "calls", "deploy_deps", "dropped", "unresolved", "_facts"):
            getattr(self, name).clear()

    @property
    def seen_rel(self) -> set:
        """Dedupe keys of the active unit: results of one file never depend on another file."""
        return self._seen.setdefault(self.store.unit, set())

    # ---- facts: what file-local extraction contributes to the cross-source checks. Persisted
    # per unit (unit_fact), so unchanged files do not need to be re-extracted.
    def _add_fact(self, kind: str, value: list, key: list | None):
        self._seq += 1
        order = [self.store.step, self.store.unit or "", self._seq]  # = full-run extraction order
        self._facts.append((order, kind, value, key))
        self.store.fact(kind, {"order": order, "value": value, "key": key})

    def load_facts(self, units: list[str]):
        for i in range(0, len(units), 500):
            part = units[i:i + 500]
            for kind, data in self.store.db.execute(
                    f"SELECT kind, data FROM unit_fact WHERE unit IN ({','.join('?' * len(part))})", part):
                d = json.loads(data)
                self._facts.append((d["order"], kind, d["value"], d.get("key")))

    def sort_facts(self):
        """Endpoints/calls/deps in full-run order; the first file to report a key wins, as before."""
        self._facts.sort(key=lambda f: f[0])
        seen = set()
        self.endpoints, self.calls, self.depends = [], [], set()
        for _, kind, value, key in self._facts:
            if key is not None:
                k = json.dumps([kind, key])
                if k in seen:
                    continue
                seen.add(k)
            if kind == "endpoint":
                self.endpoints.append(Endpoint(*value))
            elif kind == "call":
                self.calls.append(tuple(value))
            elif kind == "depends":
                self.depends.add(tuple(value))

    def seed_seen(self, unit: str):
        """Dedupe keys of another unit of the same file (the LLM pass adds what the rules missed)."""
        seen = self.seen_rel
        for kind, data in self.store.db.execute("SELECT kind, data FROM unit_fact WHERE unit = ?", (unit,)):
            key = json.loads(data).get("key")
            if kind == "endpoint" and key and key[0] == "ep":
                seen.add(tuple(key[1:]))
            elif kind == "call" and key:
                seen.add((key[1], key[2], "CALLS", key[3], key[4]))
            elif kind == "seen":
                seen.add(tuple(json.loads(data)["value"]))

    def local_sources(self):
        """Sources whose file-local results are re-extracted in this run; writes go to their unit."""
        for s in self.sources.values():
            if s.path in self.regen:
                self.store.use(f"det:{s.path}")
                yield s

    def read_blob(self, path: str) -> bytes:
        oid = self.sources[path].blob
        if oid not in self.blobs:
            self.blobs.update(gitsnap.read_blobs(self.repo, [oid]))
        return self.blobs[oid]

    # ---- components
    def owner(self, path: str) -> str | None:
        best = None
        for d, cid in self.comp_dirs.items():
            if d == "" or path == d or path.startswith(d + "/"):
                if best is None or len(d) > len(best[0]):
                    best = (d, cid)
        return best[1] if best else None

    def short(self, cid: str) -> str:
        return cid.split(":", 1)[1]

    def status_for(self, s: Source, cid: str | None = None) -> str:
        return "outdated" if s.archived or (cid in self.archived_comps) else "observed"

    def add_alias(self, alias: str | None, cid: str):
        if not alias:
            return
        a = alias.lower().strip()
        for variant in {a, a.split("/")[-1], re.sub(r"[_.\s]+", "-", a.split("/")[-1])}:
            if not variant or len(variant) < 2:
                continue
            if variant in self.aliases and self.aliases[variant] not in (cid, None):
                self.aliases[variant] = None  # two components claim the name: never guess
            else:
                self.aliases.setdefault(variant, cid)

    def add_port(self, port: int, cid: str):
        if 0 < port < 65536:
            self.ports.setdefault(port, set()).add(cid)

    # ---- resolution of "who is this address / name?"
    def resolve(self, hint: str | None) -> str | None:
        if not hint:
            return None
        hint = hint.strip().strip("\"'`")
        m = re.match(r"(?:[a-z+]+://)?([\w.-]+)(?::(\d+))?", hint)
        if re.match(r"[a-z+]+://", hint) and m:
            host, port = m.group(1).lower(), m.group(2)
            if host not in LOCAL_HOSTS:
                cid = self._alias(host.split(".")[0]) or self._alias(host)
                if cid:
                    return cid
            if port and len(self.ports.get(int(port), ())) == 1:
                return next(iter(self.ports[int(port)]))
            return None
        if re.fullmatch(r"[A-Z0-9_]+", hint):  # env var name
            stem = hint
            for p in ENV_PREFIXES:
                stem = stem.removeprefix(p)
            stem = ADDR_VAR.sub("", stem).lower().replace("_", "-")
            return self._alias(stem) or self._alias(stem + "-service") or self._fuzzy(stem)
        # explicit address evidence embedded in free text first: env var names, then URLs
        for var in re.findall(r"\b[A-Z][A-Z0-9_]{3,}\b", hint):
            if ADDR_VAR.search(var) and var != hint:
                hit = self.resolve(var)
                if hit:
                    return hit
        for url in re.findall(r"[a-z+]+://[\w.\-]+(?::\d+)?", hint):
            if url != hint:
                hit = self.resolve(url)
                if hit:
                    return hit
        direct = self._alias(hint) or self._fuzzy(hint)
        if direct:
            return direct
        # free text like "fe.cartSvcConn" or "channel passed to CartServiceStub": accept only one match
        hits = {self._alias(t) or self._fuzzy(t, loose=False) for t in re.findall(r"[A-Za-z][A-Za-z0-9]{3,}", hint)}
        hits.discard(None)
        return hits.pop() if len(hits) == 1 else None

    def _alias(self, a: str) -> str | None:
        hit = self.aliases.get(a.lower())
        if hit:
            return hit
        c = compact(a)  # separator/case-insensitive: PRODUCT_CATALOG == product-catalog == productCatalog
        hits = {cid for al, cid in self.aliases.items() if cid and compact(al) == c}
        return hits.pop() if len(hits) == 1 else None

    def _fuzzy(self, stem: str, loose: bool = True) -> str | None:
        c = compact(stem)
        while True:  # productCatalogSvcAddr -> productcatalogsvc -> productcatalog
            stripped = re.sub(r"(addr|address|url|uri|host|endpoint|client|conn|connection|stub|svc|service|api|base)$",
                              "", c)
            if stripped == c or len(stripped) < 3:
                break
            c = stripped
        if len(c) < 3:
            return None
        for cand in (c, c + "service", c + "svc", c + "api"):
            hits = {cid for al, cid in self.aliases.items() if cid and compact(al) == cand}
            if len(hits) == 1:
                return hits.pop()
        if not loose:
            return None
        hits = {cid for al, cid in self.aliases.items() if cid and (
            compact(al).startswith(c) or compact(al).endswith(c)) and len(c) >= 4}
        return hits.pop() if len(hits) == 1 else None

    # ---- shared adders (also used by enrich.py)
    def add_endpoint(self, cid: str, method: str, path: str, origin: str, ev: int, status: str,
                     confidence: float, src_path: str | None = None, rel: str = "EXPOSES") -> bool:
        method = (method or "*").upper()
        n = norm_path(path)
        key = (cid, origin, method, n)
        if key in self.seen_rel:
            return False
        self.seen_rel.add(key)
        if origin in {"code", "spec", "spec_history"}:
            eid = self.store.component(f"endpoint:{self.short(cid)} {method} {n}", "endpoint",
                                       f"{method} {path}", src_path, service=self.short(cid))
            self.store.relation(cid, rel, eid, status, confidence, ev)
        self._add_fact("endpoint", [cid, method, path, n, origin, ev], ["ep", cid, origin, method, n])
        return True

    def add_call(self, caller: str, callee: str, ev: int, status: str, confidence: float,
                 method: str = "*", path: str | None = None, extractor_rel: str = "CALLS"):
        if caller == callee:
            return
        key = (caller, callee, extractor_rel, (method or "*").upper(), norm_path(path) if path else None)
        if key in self.seen_rel:
            return
        self.seen_rel.add(key)
        props = {"method": (method or "*").upper()}
        if path:
            props["path"] = norm_path(path)
        self.store.relation(caller, extractor_rel, callee, status, confidence, ev, **props)
        if extractor_rel == "CALLS":
            fkey = ["call", caller, callee, props["method"], props.get("path")]
            self._add_fact("call", [caller, callee, ev], fkey)
            if path:
                self._add_fact("endpoint", [callee, props["method"], path, norm_path(path), "call", ev], fkey)


# --------------------------------------------------------------------------
# Components from manifests
# --------------------------------------------------------------------------

def manifest_name(s: Source) -> str | None:
    t, n = s.text or "", s.name.lower()
    try:
        if n == "package.json" or n == "composer.json":
            return json.loads(t).get("name")
    except json.JSONDecodeError:
        return None
    pats = {
        "pyproject.toml": r"^\s*name\s*=\s*[\"']([^\"']+)", "setup.cfg": r"^\s*name\s*=\s*(\S+)",
        "setup.py": r"name\s*=\s*[\"']([^\"']+)", "cargo.toml": r"^\s*name\s*=\s*[\"']([^\"']+)",
        "go.mod": r"^module\s+(\S+)", "pubspec.yaml": r"^name:\s*(\S+)", "mix.exs": r"app:\s*:(\w+)",
        "pom.xml": r"</parent>.*?<artifactId>([^<]+)|<artifactId>([^<]+)",
    }
    pat = pats.get(n)
    if pat:
        m = re.search(pat, t, re.M | re.S)
        if m:
            return next(g for g in m.groups() if g)
    return None


def discover_components(ctx: Ctx):
    st = ctx.store
    dirs: dict[str, list[Source]] = {}
    for s in ctx.sources.values():
        if s.kind == "dependency_manifest":
            dirs.setdefault(s.path.rsplit("/", 1)[0] if "/" in s.path else "", []).append(s)
    if len(dirs) > 1:
        dirs.pop("", None)  # the root manifest of a multi-project repo is a workspace, not a component
    # a test project (e.g. src/cartservice/tests) belongs to the component it tests
    for d in [d for d in dirs if d and d.rsplit("/", 1)[-1].lower() in TEST_SEGS]:
        dirs.pop(d)
    # a manifest in a generic folder (src/, app/, server/) names its parent folder's component
    merged: dict[str, list[Source]] = {}
    for d, files in dirs.items():
        while d and "/" in d and d.rsplit("/", 1)[-1].lower() in GENERIC_DIRS:
            d = d.rsplit("/", 1)[0]
        merged.setdefault(d, []).extend(files)
    dirs = merged
    if not dirs:
        dirs[""] = []      # single-project repo without a manifest
    taken: set[str] = set()
    for d in sorted(dirs):
        files = dirs[d]
        name = next((n for n in (manifest_name(f) for f in files) if n), None) or d.rsplit("/", 1)[-1] or ctx.repo_name
        short = (d.rsplit("/", 1)[-1] or ctx.repo_name).lower()
        cid = f"comp:{short}"
        if cid in taken:
            cid = f"comp:{d.lower()}"
        taken.add(cid)
        archived = bool({p.lower() for p in d.split("/")} & ARCHIVE_SEGS)
        st.component(cid, "project", name, d or ".", archived=archived,
                     manifests=[f.name for f in files])
        ctx.comp_dirs[d] = cid
        if archived:
            ctx.archived_comps.add(cid)
        for a in (name, short, name.split("/")[-1].lstrip("@")):
            ctx.add_alias(a, cid)


def extract_manifests(ctx: Ctx):
    st = ctx.store
    for s in ctx.local_sources():
        if s.kind != "dependency_manifest" or not s.text:
            continue
        src = ctx.comp_dirs.get(s.path.rsplit("/", 1)[0] if "/" in s.path else "")
        if not src:
            continue
        status = ctx.status_for(s, src)
        for dep, ver, dev, ln in parse_deps(s):
            ev = st.evidence(s.path, ln, s.lines[ln - 1] if ln else f"{dep} {ver}", "manifest")
            internal = ctx._alias(dep) if (ver.startswith(("workspace:", "file:", "link:", "path:"))
                                           or ctx._alias(dep)) else None
            if internal and internal != src:
                st.relation(src, "DEPENDS_ON", internal, status, 0.95, ev, dev=dev, version=ver)
                ctx._add_fact("depends", [src, internal], None)
            else:
                lib = st.component(f"lib:{dep.lower()}", "library-ext", dep)
                st.relation(src, "USES_LIBRARY", lib, status, 0.95, ev, version=ver, dev=dev)


def parse_deps(s: Source):
    """Yield (name, version, is_dev, line) for the common manifest formats."""
    n, t = s.name.lower(), s.text
    if n in {"package.json", "composer.json"}:
        try:
            data = json.loads(t)
        except json.JSONDecodeError:
            return
        for sec in ("dependencies", "devDependencies", "peerDependencies", "require", "require-dev"):
            for dep, ver in (data.get(sec) or {}).items():
                yield dep, str(ver), "dev" in sec.lower(), line_of(s, f'"{dep}"')
    elif n in {"requirements.txt", "pipfile"} or n.endswith(".txt"):
        for i, line in enumerate(s.lines, 1):
            m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9_.\-]*)(\[[^\]]*\])?\s*([=<>!~]=?\s*[^\s;#]+)?", line)
            if m and not line.strip().startswith(("#", "-")):
                yield m.group(1).lower(), (m.group(3) or "").strip(), False, i
    elif n == "pyproject.toml":
        block = re.search(r"dependencies\s*=\s*\[(.*?)\]", t, re.S)
        for m in re.finditer(r"[\"']([A-Za-z0-9][A-Za-z0-9_.\-]*)([^\"']*)[\"']", block.group(1) if block else ""):
            yield m.group(1).lower(), m.group(2).strip(), False, line_of(s, m.group(0))
        for m in re.finditer(r"^([A-Za-z0-9][\w.-]*)\s*=\s*[\"'{]", t.split("[tool.poetry.dependencies]", 1)[-1]
                             if "[tool.poetry.dependencies]" in t else "", re.M):
            if m.group(1) != "python":
                yield m.group(1).lower(), "", False, line_of(s, m.group(0))
    elif n == "go.mod":
        for i, line in enumerate(s.lines, 1):
            m = re.match(r"\s*(?:require\s+)?([\w.\-]+\.[\w.\-/]+)\s+(v[\w.\-+]+)", line)
            if m:
                yield m.group(1), m.group(2), False, i
    elif n == "pom.xml":
        for m in re.finditer(r"<dependency>\s*<groupId>([^<]+)</groupId>\s*<artifactId>([^<]+)</artifactId>"
                             r"(?:\s*<version>([^<]+)</version>)?", t):
            yield m.group(2), m.group(3) or "", False, line_at(s, m.start())
    elif n in {"build.gradle", "build.gradle.kts"}:
        for m in re.finditer(r"(implementation|api|compileOnly|runtimeOnly|testImplementation)\s*\(?\s*[\"']"
                             r"([\w.\-]+):([\w.\-]+):?([\w.\-]*)", t):
            yield m.group(3), m.group(4), m.group(1).startswith("test"), line_at(s, m.start())
        for m in re.finditer(r"project\(\s*[\"']:([\w.\-:]+)[\"']", t):
            yield m.group(1).split(":")[-1], "path:", False, line_at(s, m.start())
    elif n == "cargo.toml":
        for sec in re.finditer(r"^\[((?:dev-)?dependencies)\]\s*$(.*?)(?=^\[|\Z)", t, re.M | re.S):
            for m in re.finditer(r"^([\w\-]+)\s*=\s*(.+)$", sec.group(2), re.M):
                ver = "path:" if "path" in m.group(2) else m.group(2).strip()
                yield m.group(1), ver, sec.group(1).startswith("dev"), line_of(s, m.group(0))
    elif n == "gemfile":
        for i, line in enumerate(s.lines, 1):
            m = re.match(r"\s*gem\s+[\"']([^\"']+)[\"'](?:\s*,\s*[\"']([^\"']+))?", line)
            if m:
                yield m.group(1), m.group(2) or "", False, i


# --------------------------------------------------------------------------
# Addresses: where each component listens / is reachable
# --------------------------------------------------------------------------

def extract_addresses(ctx: Ctx):
    st = ctx.store
    for s in ctx.sources.values():
        if not s.text:
            continue
        cid = ctx.owner(s.path)
        if s.name.lower().startswith(".env") and cid and cid != ctx.comp_dirs.get(""):
            m = re.search(r"^(?:export\s+)?PORT\s*=\s*(\d+)", s.text, re.M)
            if m:
                ctx.add_port(int(m.group(1)), cid)
        elif s.name.lower().startswith("dockerfile") and cid and cid != ctx.comp_dirs.get(""):
            for m in re.finditer(r"^EXPOSE\s+(\d+)", s.text, re.M):
                ctx.add_port(int(m.group(1)), cid)
        elif s.name.endswith(".json") and s.kind == "config":
            # a "port registry": an object whose keys are component names and values are ports
            try:
                data = json.loads(s.text)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                hits = {k: v for k, v in data.items() if isinstance(v, int) and 0 < v < 65536 and ctx._alias(k)}
                if len(hits) >= 2:
                    for k, v in hits.items():
                        ctx.add_port(v, ctx._alias(k))
                        st.evidence(s.path, line_of(s, f'"{k}"'), f'"{k}": {v}', "port-registry")


def load_yaml_docs(s: Source) -> list:
    if yaml is None or not s.text:
        return []
    try:
        return [d for d in yaml.safe_load_all(s.text) if isinstance(d, dict)]
    except yaml.YAMLError:
        return []


def env_items(env) -> list[tuple[str, str]]:
    if isinstance(env, dict):
        return [(str(k), str(v)) for k, v in env.items() if v is not None]
    if isinstance(env, list):
        out = []
        for e in env:
            if isinstance(e, str) and "=" in e:
                out.append(tuple(e.split("=", 1)))
            elif isinstance(e, dict) and "name" in e:
                out.append((str(e["name"]), str(e.get("value", ""))))
        return out
    return []


def extract_deployment(ctx: Ctx):
    """docker compose + Kubernetes manifests, via a real YAML parser."""
    st = ctx.store
    for s in ctx.sources.values():
        if s.kind != "deployment" or not s.path.endswith((".yaml", ".yml")):
            continue
        for doc in load_yaml_docs(s):
            if isinstance(doc.get("services"), dict):
                compose_services(ctx, s, doc["services"])
            elif doc.get("kind") in {"Deployment", "StatefulSet", "DaemonSet", "Service", "Job", "CronJob"}:
                k8s_object(ctx, s, doc)
    _ = st


def compose_services(ctx: Ctx, s: Source, services: dict):
    st = ctx.store
    mapping: dict[str, str] = {}
    for name, svc in services.items():
        svc = svc or {}
        build = svc.get("build")
        cid = ctx._alias(name)
        if not cid and isinstance(build, dict):
            ctx_dir = str(build.get("context", "")).strip("./")
            for v in (build.get("args") or {}).values() if isinstance(build.get("args"), dict) else []:
                cid = cid or ctx._alias(str(v))
            cid = cid or (ctx.owner(ctx_dir + "/x") if ctx_dir else None)
        elif not cid and isinstance(build, str):
            cid = ctx.owner(build.strip("./") + "/x")
        if not cid and svc.get("image"):
            cid = ctx._alias(str(svc["image"]).split(":")[0].split("/")[-1])
        if cid and cid != ctx.comp_dirs.get(""):
            mapping[name] = cid
            ctx.add_alias(name, cid)
    for name, svc in services.items():
        cid = mapping.get(name)
        if not cid:
            continue
        svc = svc or {}
        for p in svc.get("ports") or []:
            m = re.search(r"(?:(\d+)[:])?(\d+)(?:/\w+)?$", str(p if not isinstance(p, dict) else p.get("published", "")))
            if m:
                ctx.add_port(int(m.group(1) or m.group(2)), cid)
        deps = svc.get("depends_on") or []
        for dep in (deps.keys() if isinstance(deps, dict) else deps):
            target = mapping.get(dep) or ctx._alias(dep)
            if target:
                start = line_of(s, f"{name}:") or 0
                dline = line_of(s, "depends_on", start)
                ln = line_of(s, dep, dline - 1) if dline else line_of(s, dep, start)
                ev = st.evidence(s.path, ln, s.lines[ln - 1] if ln else f"{name} depends_on {dep}", "compose")
                st.relation(cid, "DEPLOY_DEPENDS_ON", target, "observed", 0.95, ev)
                ctx.deploy_deps.append((cid, target, ev))
        for key, value in env_items(svc.get("environment")):
            if re.search(r"https?://", value) or ADDR_VAR.search(key):
                target = ctx.resolve(value) or ctx.resolve(key)
                if target:
                    ln = line_of(s, key, line_of(s, f"{name}:") or 0)
                    ev = st.evidence(s.path, ln, s.lines[ln - 1] if ln else f"{key}: {value}", "compose")
                    ctx.add_call(cid, target, ev, "observed", 0.9, extractor_rel="CONFIGURED_TO_CALL")


def k8s_object(ctx: Ctx, s: Source, doc: dict):
    st = ctx.store
    name = (doc.get("metadata") or {}).get("name", "")
    cid = ctx._alias(name) or ctx._fuzzy(name.lower())
    if not cid:
        return
    spec = doc.get("spec") or {}
    tmpl = (spec.get("template") or {}).get("spec") or spec.get("jobTemplate", {}).get("spec", {}) or {}
    for c in tmpl.get("containers") or []:
        for p in c.get("ports") or []:
            if isinstance(p, dict) and p.get("containerPort"):
                ctx.add_port(int(p["containerPort"]), cid)
        for key, value in env_items(c.get("env")):
            if re.search(r"https?://|^[\w-]+:\d+$", value) or ADDR_VAR.search(key):
                target = ctx.resolve(value if "://" in value else f"http://{value}") or ctx.resolve(key)
                if target:
                    ln = line_of(s, key)
                    ev = st.evidence(s.path, ln, s.lines[ln - 1] if ln else f"{key}={value}", "kubernetes")
                    ctx.add_call(cid, target, ev, "observed", 0.9, extractor_rel="CONFIGURED_TO_CALL")


# --------------------------------------------------------------------------
# Code: common-framework fast path (the LLM code pass in enrich.py covers the rest)
# --------------------------------------------------------------------------

ROUTE_PATTERNS = [
    # Express / Koa / Fastify / Hono:  app.get("/x"), router.post('/x')
    re.compile(r"\b(?:app|router|server|api|route[rs]?)\.(get|post|put|patch|delete|all|use)\(\s*[\"'`](/[^\"'`]*)[\"'`]"),
    # Flask / FastAPI / Starlette / Sanic:  @app.get("/x"), @router.route("/x")
    re.compile(r"@\w+\.(get|post|put|patch|delete|route|api_route)\(\s*[\"'](/[^\"']*)[\"']"),
    # Spring:  @GetMapping("/x"), @RequestMapping(value = "/x")
    re.compile(r"@(Get|Post|Put|Patch|Delete|Request)Mapping\(\s*(?:value\s*=\s*|path\s*=\s*)?[\"'](/[^\"']*)[\"']"),
    # Go net/http, chi, gin, echo:  HandleFunc("/x"), r.GET("/x")
    re.compile(r"\.(HandleFunc|Handle|GET|POST|PUT|PATCH|DELETE|Get|Post|Put|Patch|Delete)\(\s*\"(/[^\"]*)\""),
    # ASP.NET:  [HttpGet("/x")], app.MapGet("/x")
    re.compile(r"(?:\[Http(Get|Post|Put|Patch|Delete)\(\s*\"|\.Map(Get|Post|Put|Patch|Delete)\(\s*\")(/?[^\"]*)\""),
]
ENV_READ = re.compile(
    r"process\.env\.(\w+)|process\.env\[[\"'](\w+)[\"']\]|import\.meta\.env\.(\w+)|os\.environ(?:\.get)?[\(\[]\s*[\"'](\w+)"
    r"|os\.getenv\(\s*[\"'](\w+)|System\.getenv\(\s*\"(\w+)|os\.Getenv\(\s*\"(\w+)|\benv\(\s*[\"'](\w+)"
    r"|ENV\[[\"'](\w+)|Environment\.GetEnvironmentVariable\(\s*\"(\w+)|std::env::var\(\s*\"(\w+)")
URL_LITERAL = re.compile(r"[\"'`](https?://[\w.\-]+(?::\d+)?)(/[^\"'`\s]*)?[\"'`]")
SQL_STRING = re.compile(r"[\"'`]((?:SELECT|INSERT|UPDATE|DELETE)\b[^\"'`]*)[\"'`]", re.I)
SQL_TABLE = re.compile(r"\b(FROM|INTO|UPDATE|JOIN)\s+[\"`]?(\w+)", re.I)
METHOD_HINT = re.compile(r"method\s*[:=]\s*[\"'](\w+)|\.(get|post|put|patch|delete)\(|\b(GET|POST|PUT|PATCH|DELETE)\b", re.I)


def extract_code(ctx: Ctx):
    st = ctx.store
    for s in ctx.local_sources():
        if s.kind not in {"code", "test"} or not s.text:
            continue
        src = ctx.owner(s.path)
        if not src:
            continue
        status = ctx.status_for(s, src)

        if s.kind == "code":
            for pat in ROUTE_PATTERNS:
                for m in pat.finditer(s.text):
                    groups = [g for g in m.groups() if g]
                    path = groups[-1]
                    method = groups[0].upper() if len(groups) > 1 else "*"
                    method = "*" if method in {"ALL", "USE", "ROUTE", "API_ROUTE", "REQUEST", "HANDLEFUNC", "HANDLE"} else method
                    if not path.startswith("/"):
                        path = "/" + path
                    if path == "/":
                        continue
                    ln = line_at(s, m.start())
                    ev = st.evidence(s.path, ln, s.lines[ln - 1], "route-pattern")
                    ctx.add_endpoint(src, method, path, "code", ev, status, 0.9, s.path)

        # outbound calls: env-configured base URLs, bound variables, literal URLs
        bound: dict[str, str] = {}
        for i, line in enumerate(s.lines, 1):
            for m in ENV_READ.finditer(line):
                var = next(g for g in m.groups() if g)
                if not ADDR_VAR.search(var):
                    continue
                default = URL_LITERAL.search(line)
                target = ctx.resolve(var) or (ctx.resolve(default.group(1)) if default else None)
                if not target:
                    continue
                ev = st.evidence(s.path, i, line, "env-address")
                ctx.add_call(src, target, ev, status, 0.8)
                b = re.match(r"\s*(?:export\s+)?(?:const|let|var|final|val|private|static|public|\w+\s+)*\s*"
                             r"([A-Za-z_]\w*)\s*(?::[^=]+)?=", line)
                if b:
                    bound[b.group(1)] = target
            for m in URL_LITERAL.finditer(line):
                if ENV_READ.search(line):
                    continue
                target = ctx.resolve(m.group(1))
                if target and m.group(2):
                    ev = st.evidence(s.path, i, line, "url-literal")
                    ctx.add_call(src, target, ev, status, 0.75, _method(s, i), m.group(2))
        for var, target in bound.items():
            v = re.escape(var)
            for m in re.finditer(r"\$\{\s*" + v + r"\s*\}(/(?:\$\{[^}]*\}|[^`\"'\s,)$])*)"     # JS template
                                 r"|\b" + v + r"\s*\+\s*[\"'](/[^\"']*)[\"']"                   # concatenation
                                 r"|f[\"'][^\"']*\{" + v + r"\}(/[^\"']*)[\"']", s.text):    # python f-string
                raw = next(g for g in m.groups() if g)
                path = re.sub(r"\$\{[^}]*\}|\{[^}]*\}", "{}", raw)
                if path.endswith("/"):
                    path += "{}"
                ln = line_at(s, m.start())
                ev = st.evidence(s.path, ln, s.lines[ln - 1], "bound-address")
                ctx.add_call(src, target, ev, status, 0.85, _method(s, ln), path)

        # SQL executed by the code
        for m in SQL_STRING.finditer(s.text):
            sql = m.group(1)
            ln = line_at(s, m.start())
            for tm in SQL_TABLE.finditer(sql):
                op = "READS" if sql.lstrip().upper().startswith("SELECT") else "WRITES"
                tid = st.component(f"table:{ctx.short(src)}.{tm.group(2)}", "table", tm.group(2), None,
                                   service=ctx.short(src))
                key = (src, op, tid, ln, s.path)
                if key not in ctx.seen_rel:
                    ctx.seen_rel.add(key)
                    st.relation(src, op, tid, status, 0.85, st.evidence(s.path, ln, sql, "sql-in-code"))
                    ctx._add_fact("seen", list(key), None)  # lets the LLM pass skip it


def _method(s: Source, ln: int) -> str:
    window = "\n".join(s.lines[ln - 1: ln + 6])
    m = METHOD_HINT.search(window)
    if m:
        return next(g for g in m.groups() if g).upper()
    return "*"


# --------------------------------------------------------------------------
# Specs, schemas, ownership, docs
# --------------------------------------------------------------------------

def load_spec(s: Source):
    if s.path.endswith(".json"):
        try:
            return json.loads(s.text)
        except json.JSONDecodeError:
            return None
    docs = load_yaml_docs(s)
    return docs[0] if docs else None


def extract_specs(ctx: Ctx):
    st = ctx.store
    for s in ctx.local_sources():
        if s.kind != "api_spec":
            continue
        spec = load_spec(s)
        if not isinstance(spec, dict):
            continue
        stem = s.name.split(".")[0]
        cid = None
        for srv in spec.get("servers") or []:
            cid = cid or ctx.resolve(str(srv.get("url", "")))
        if spec.get("host"):
            cid = cid or ctx.resolve(f"http://{spec['host']}")
        cid = cid or ctx.resolve(stem) or ctx.owner(s.path)
        if not cid or cid == ctx.comp_dirs.get(""):
            cid = st.component(f"spec-only:{stem}", "unknown-service", stem)
        origin = "spec_history" if s.archived else "spec"
        status = "outdated" if s.archived else "declared"
        for path, ops in (spec.get("paths") or {}).items():
            if not isinstance(ops, dict):
                continue
            ln = line_of(s, f'"{path}"') or line_of(s, f"{path}:")
            for method in ops:
                if method.lower() in {"get", "post", "put", "patch", "delete", "head", "options"}:
                    ev = st.evidence(s.path, ln, f"{method.upper()} {path}", "openapi")
                    ctx.add_endpoint(cid, method, path, origin, ev, status, 0.8 if not s.archived else 0.5,
                                     None, rel="DECLARES")


def extract_schemas(ctx: Ctx):
    """Replay each migration folder in an in-memory SQLite to get the real final schema."""
    st = ctx.store
    by_dir: dict[str, list[Source]] = {}
    for s in ctx.sources.values():
        if s.kind == "schema" and s.path.endswith(".sql"):
            by_dir.setdefault(s.path.rsplit("/", 1)[0] if "/" in s.path else "", []).append(s)
    for d, files in sorted(by_dir.items()):
        cid = ctx.owner(d + "/x") or st.component(f"comp:{d or ctx.repo_name}", "project", d or ctx.repo_name, d)
        mem = sqlite3.connect(":memory:")
        prev: dict[str, set[str]] = {}
        failed = False
        files = sorted(files, key=lambda x: x.path)
        for s in files:
            try:
                mem.executescript(s.text)
            except sqlite3.Error as e:
                failed = True
                print(f"  note: {s.path} is not SQLite-compatible ({e}); schema left to the LLM pass")
                break
            now = {t: {r[1] for r in mem.execute(f"PRAGMA table_info('{t}')")}
                   for (t,) in mem.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for t, cols in prev.items():
                for c in cols - now.get(t, set()):
                    ln = (line_of(s, f"DROP COLUMN {c}") or line_of(s, f"RENAME COLUMN {c} ")
                          or line_of(s, f" {c} ") or 1)
                    ctx.dropped.append((cid, t, c, st.evidence(s.path, ln, s.lines[ln - 1], "migration-replay")))
            prev = now
        if failed and not prev:
            continue
        ctx.tables[cid] = prev
        last = files[-1]
        for t, cols in prev.items():
            ev = st.evidence(last.path, None, f"final schema of {t}: {', '.join(sorted(cols))}", "migration-replay")
            # outranks the sql-in-code declaration of the same table, so the live columns win
            tid = st.component(f"table:{ctx.short(cid)}.{t}", "table", t, None, service=ctx.short(cid),
                               columns=sorted(cols))
            st.relation(cid, "HAS_TABLE", tid, "outdated" if last.archived else "observed", 0.95, ev)


def codeowners_match(pattern: str, path: str) -> bool:
    p = pattern.strip()
    anchored = p.startswith("/")
    p = p.strip("/")
    if not p or p == "*":
        return True
    if any(ch in p for ch in "*?["):
        return fnmatch.fnmatch(path, p) or fnmatch.fnmatch(path, p + "/*") or \
            (not anchored and fnmatch.fnmatch(path.rsplit("/", 1)[-1], p))
    return path == p or path.startswith(p + "/") or (not anchored and ("/" + p + "/") in ("/" + path + "/"))


def extract_owners(ctx: Ctx):
    st = ctx.store
    files = [s for s in ctx.sources.values() if s.kind == "ownership"]
    if not files:
        return
    s = files[0]
    rules = []
    for i, line in enumerate(s.lines, 1):
        if line.strip() and not line.lstrip().startswith("#"):
            pattern, *owners = line.split()
            rules.append((pattern, owners, i, line))
    all_paths = list(ctx.sources)
    for pattern, owners, i, line in rules:
        if not any(codeowners_match(pattern, p) for p in all_paths):
            ev = st.evidence(s.path, i, line, "codeowners")
            st.conflict("stale", "codeowners-dangling", pattern,
                        f"CODEOWNERS assigns {pattern} but no file matches it", 0.8, [ev])
    for d, cid in ctx.comp_dirs.items():
        winner = None
        for rule in rules:  # last matching rule wins, like git
            if codeowners_match(rule[0], d or "."):
                winner = rule
        if winner:
            ev = st.evidence(s.path, winner[2], winner[3], "codeowners")
            for o in winner[1]:
                st.relation(cid, "OWNED_BY", st.component(f"team:{o}", "team", o), "observed", 0.95, ev)
                ctx.owned.add(cid)


DOC_KINDS = {"doc", "wiki", "adr", "runbook", "meeting_note", "incident", "issue", "release_note",
             "api_doc", "design_doc", "readme"}
DOC_ENDPOINT = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE)\s+(/[A-Za-z0-9_/{}:.\-]+)")


def extract_docs(ctx: Ctx):
    st = ctx.store
    names = sorted(((a, cid) for a, cid in ctx.aliases.items() if cid and len(a) >= 4), key=lambda x: -len(x[0]))
    for s in ctx.local_sources():
        if s.kind not in DOC_KINDS or not s.text:
            continue
        hit = set()
        for alias, cid in names:
            if cid in hit:
                continue
            m = re.search(rf"(?<![\w/-]){re.escape(alias)}(?![\w-])", s.text, re.I)
            if m:
                hit.add(cid)
                ln = line_at(s, m.start())
                ev = st.evidence(s.path, ln, s.lines[ln - 1], "doc-mention")
                st.relation(cid, "MENTIONED_IN", f"source:{s.path}", "declared", 0.4, ev, kind=s.kind, date=s.doc_date)
        for m in DOC_ENDPOINT.finditer(s.text):
            ln = line_at(s, m.start())
            ev = st.evidence(s.path, ln, s.lines[ln - 1], "doc-endpoint")
            ctx._add_fact("endpoint", ["?", m.group(1), m.group(2), norm_path(m.group(2)), "doc", ev], None)


def finalize_components(ctx: Ctx):
    """service = exposes routes or listens on a port; library = others depend on it; else project."""
    st = ctx.store
    exposes = {e.service for e in ctx.endpoints if e.origin == "code"}
    listened = {c for cids in ctx.ports.values() for c in cids}
    depended = {dst for _, dst in ctx.depends}
    final: dict[str, tuple[str, dict]] = {}
    for cid in ctx.comp_dirs.values():
        decl = st.declared(cid)
        if decl:
            kind = "service" if cid in exposes or cid in listened else "library" if cid in depended else "project"
            final[cid] = (kind, json.loads(decl[3] or "{}"))
    for port, cids in ctx.ports.items():
        for cid in cids:
            if cid in final:
                ports = final[cid][1].setdefault("ports", [])
                if port not in ports:
                    ports.append(port)
    for cid, (kind, props) in final.items():
        st.redeclare(cid, kind, props)


# --------------------------------------------------------------------------
# Cross-checks
# --------------------------------------------------------------------------

def find(eps, service, method, norm, origins):
    return [e for e in eps if e.origin in origins and (service in (None, e.service))
            and (method == "*" or e.method in (method, "*")) and path_match(e.norm, norm)]


def check_endpoints(ctx: Ctx):
    st, eps = ctx.store, ctx.endpoints
    implemented = {e.service for e in eps if e.origin == "code"}
    specified = {e.service for e in eps if e.origin == "spec"}
    for e in eps:
        if e.norm in {"/health", "/healthz", "/ready", "/readyz", "/live", "/livez", "/metrics"}:
            continue
        name = ctx.short(e.service) if ":" in e.service else e.service
        if e.origin == "spec" and e.service in implemented and not find(eps, e.service, e.method, e.norm, {"code"}):
            st.conflict("conflict", "spec-not-implemented", f"{name} {e.method} {e.path}",
                        f"An API spec declares {e.method} {e.path} on {name}, but no matching route was found in its code",
                        0.75, [e.ev])
        elif e.origin == "code" and e.service in specified and not find(eps, e.service, e.method, e.norm, {"spec"}):
            st.conflict("gap", "route-not-in-spec", f"{name} {e.method} {e.path}",
                        f"{name} implements {e.method} {e.path} but its API spec does not document it", 0.7, [e.ev])
        elif e.origin == "spec_history":
            live = find(eps, e.service, e.method, e.norm, {"code"})
            if live:
                st.conflict("stale", "history-spec-still-live", f"{name} {e.method} {e.path}",
                            f"An archived spec lists {e.method} {e.path} and the route is still live in code",
                            0.5, [e.ev, live[0].ev])
        elif e.origin == "call" and e.service in implemented and not find(eps, e.service, e.method, e.norm, {"code"}):
            st.conflict("conflict", "call-to-missing-route", f"{name} {e.method} {e.path}",
                        f"Code calls {e.method} {e.path} on {name}, which does not implement it (contract break)",
                        0.8, [e.ev])
        elif e.origin == "doc" and implemented and not find(eps, None, e.method, e.norm, {"code"}):
            hist = find(eps, None, e.method, e.norm, {"spec_history"})
            note = " It only appears in an archived spec." if hist else ""
            st.conflict("stale", "doc-endpoint-not-implemented", f"{e.method} {e.path}",
                        f"Documentation mentions {e.method} {e.path} but no service implements it.{note}",
                        0.65 if hist else 0.5, [e.ev] + [h.ev for h in hist])


def check_deploy_vs_code(ctx: Ctx):
    st = ctx.store
    declared = {(a, b) for a, b, _ in ctx.deploy_deps}
    deployed = {a for a, _, _ in ctx.deploy_deps} | {b for _, b, _ in ctx.deploy_deps}
    if not ctx.deploy_deps:
        return
    seen = set()
    for caller, callee, ev in ctx.calls:
        if (caller, callee) in seen or caller not in deployed or callee not in deployed:
            continue
        seen.add((caller, callee))
        if (caller, callee) not in declared:
            st.conflict("gap", "call-not-in-deploy-deps", f"{ctx.short(caller)} -> {ctx.short(callee)}",
                        f"{ctx.short(caller)} calls {ctx.short(callee)} in code, but the deployment config does not "
                        "declare that dependency (startup order and outage blast radius are invisible there)", 0.6, [ev])


def check_schema_drift(ctx: Ctx):
    st = ctx.store
    for cid, table, col, drop_ev in ctx.dropped:
        live = ctx.tables.get(cid, {}).get(table, set())
        if col in live:
            continue
        for s in ctx.sources.values():
            if not s.text or s.kind in {"schema", "fixture", "lockfile", "binary"}:
                continue
            if s.kind in {"code", "test"} and ctx.owner(s.path) == cid:
                for m in SQL_STRING.finditer(s.text):
                    if re.search(rf"\b{table}\b", m.group(1)) and re.search(rf"\b{col}\b(?!_)", m.group(1)):
                        ln = line_at(s, m.start())
                        ev = st.evidence(s.path, ln, m.group(1), "schema-drift")
                        st.conflict("conflict", "code-uses-dropped-column", f"{ctx.short(cid)}.{table}.{col}",
                                    f"Code references {table}.{col}, which a migration dropped or renamed", 0.85,
                                    [ev, drop_ev])
            elif s.kind in DOC_KINDS:
                m = re.search(rf"`{col}`|\b{col}\s*:|\b{col}\s+(?:field|column|property)\b|\b{table}\.{col}\b", s.text)
                low = s.text.lower()
                if m and (ctx.short(cid) in low or table.lower() in low):
                    ln = line_at(s, m.start())
                    ev = st.evidence(s.path, ln, s.lines[ln - 1], "schema-drift")
                    st.conflict("stale", "doc-describes-dropped-column", f"{ctx.short(cid)}.{table}.{col}",
                                f"{s.path} ({s.doc_date or s.git_date or 'undated'}) describes field '{col}', but "
                                f"migrations dropped/renamed {table}.{col}; live columns: {', '.join(sorted(live))}. "
                                "Candidate - verified in stage 2.", 0.5, [ev, drop_ev])


def check_ownership(ctx: Ctx):
    st = ctx.store
    if not any(s.kind == "ownership" for s in ctx.sources.values()):
        return
    co = next(s for s in ctx.sources.values() if s.kind == "ownership")
    for d, cid in ctx.comp_dirs.items():
        if cid not in ctx.owned and cid not in ctx.archived_comps:
            ev = st.evidence(co.path, None, f"no CODEOWNERS rule matches {d or '.'}", "codeowners")
            st.conflict("gap", "no-owner", ctx.short(cid),
                        f"{ctx.short(cid)} ({d}) has no CODEOWNERS entry - nobody to page on outage", 0.85, [ev])


# --------------------------------------------------------------------------
# Package surface: npm export entrypoints, tsconfig presets, typecheck scripts.
# Manifest parsing only recorded dependency pins, so a broken export, a preset
# nobody extends, or a compiler script aimed at a different package never became
# a finding.
# --------------------------------------------------------------------------

_EXPORT_CONDITIONS = {"import", "require", "default", "types", "node", "browser", "development", "production"}
_NODE_BUILTINS = {
    "assert", "async_hooks", "buffer", "child_process", "cluster", "console", "constants", "crypto",
    "dgram", "dns", "domain", "events", "fs", "http", "http2", "https", "inspector", "module", "net",
    "os", "path", "perf_hooks", "process", "punycode", "querystring", "readline", "repl", "stream",
    "string_decoder", "sys", "timers", "tls", "trace_events", "tty", "url", "util", "v8", "vm", "wasi",
    "worker_threads", "zlib",
}
_FRAMEWORKS = {"react": "react", "remix": "remix", "vite": "vite", "next": "next", "nextjs": "next",
               "vue": "vue", "svelte": "svelte", "angular": "angular"}
_SPEC_RE = re.compile(
    r"""(?:^|[^\w$])(?:import|export)\s+(?:type\s+)?(?:[^'"]*?\sfrom\s+)?['"]([^'"]+)['"]"""
    r"""|(?:^|[^\w$])require(?:\.resolve)?\(\s*['"]([^'"]+)['"]\s*\)"""
    r"""|(?:^|[^\w$])import\(\s*['"]([^'"]+)['"]\s*\)""")
_MAP_RESOLVE_RE = re.compile(r"\[(?P<body>[^\[\]]*)\]\s*\.map\(\s*require(?:\.resolve)?\s*\)", re.S)
_COMPILER_RE = re.compile(r"\b(tsc6|tsgo|tsc)\b")
_DEP_SECTIONS = ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies")


def _loads_jsonc(text: str):
    """JSON with // and /* */ comments and trailing commas, as tsconfig files use."""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            out.append(c)
            i += 1
            while i < n:
                out.append(text[i])
                if text[i] == "\\":
                    i += 1
                    if i < n:
                        out.append(text[i])
                elif text[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            i = text.find("\n", i)
            if i < 0:
                break
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            end = text.find("*/", i + 2)
            i = n if end < 0 else end + 2
            continue
        out.append(c)
        i += 1
    raw = re.sub(r",(\s*[}\]])", r"\1", "".join(out))
    return json.loads(raw)


def _rel_join(base: str, rel: str) -> str:
    parts = [p for p in base.split("/") if p] if base else []
    for p in rel.split("/"):
        if p in ("", "."):
            continue
        if p == "..":
            if parts:
                parts.pop()
        else:
            parts.append(p)
    return "/".join(parts)


def _flatten_exports(node, subpath="."):
    if isinstance(node, str):
        yield subpath, node
    elif isinstance(node, dict):
        if any(str(k).startswith(".") for k in node):
            for k, v in node.items():
                yield from _flatten_exports(v, k)
        else:
            for v in node.values():
                yield from _flatten_exports(v, subpath)


def _export_key(key: str) -> str:
    if key in {".", "./"}:
        return "."
    return key[2:] if key.startswith("./") else key


def _specifier_package(spec: str) -> str | None:
    if not spec or spec.startswith((".", "/", "node:", "#", "file:", "data:", "http:", "https:")):
        return None
    if spec.startswith("@"):
        parts = spec.split("/")
        return "/".join(parts[:2]) if len(parts) >= 2 else None
    base = spec.split("/")[0]
    return None if base in _NODE_BUILTINS else base


def _import_key(spec: str, pkg: str) -> str | None:
    if spec == pkg:
        return "."
    prefix = pkg + "/"
    return spec[len(prefix):] if spec.startswith(prefix) else None


def _js_specifiers(text: str) -> list[tuple[int, str]]:
    found = []
    for i, line in enumerate(text.splitlines(), 1):
        for m in _SPEC_RE.finditer(line):
            found.append((i, next(g for g in m.groups() if g)))
    for m in _MAP_RESOLVE_RE.finditer(text):
        body, start = m.group("body"), m.start("body")
        for sm in re.finditer(r"""['"]([^'"]+)['"]""", body):
            found.append((text.count("\n", 0, start + sm.start()) + 1, sm.group(1)))
    return found


def _export_bindings(text: str) -> tuple[set[str], bool, bool]:
    names = set(re.findall(
        r"(?m)^export\s+(?:async\s+)?(?:const|let|var|function|class)\s+([A-Za-z_$][\w$]*)", text))
    for block in re.findall(r"(?m)^export\s*\{([^}]+)\}", text):
        for part in block.split(","):
            part = part.strip()
            if not part:
                continue
            names.add(part.split(" as ")[-1].strip() if " as " in part else part.split()[0])
    has_default = bool(re.search(r"(?m)^export\s+default\b", text))
    return names, has_default, "module.exports" in text


def _named_imports(line: str) -> tuple[set[str], bool]:
    names = set()
    m = re.search(r"import\s*\{([^}]+)\}", line)
    if m:
        for part in m.group(1).split(","):
            part = part.strip()
            if part:
                names.add(re.split(r"\s+as\s+", part)[0].strip())
    default = bool(re.match(r"import\s+[A-Za-z_$]", line.strip()))
    return names, default


def _frameworks(text: str) -> set[str]:
    return {_FRAMEWORKS[t] for t in re.findall(r"[A-Za-z]+", text.lower()) if t in _FRAMEWORKS}


def _comment_text(text: str) -> str:
    return "\n".join(re.findall(r"/\*([\s\S]*?)\*/", text) + re.findall(r"(?m)^\s*//(.*)$", text))


def _quote(s: Source, needle: str, fallback: int = 1) -> tuple[int, str]:
    ln = line_of(s, needle) or fallback
    return ln, s.lines[ln - 1] if 0 < ln <= len(s.lines) else needle


def _ev(st, s: Source, needle: str, extractor: str, fallback: int = 1) -> int:
    ln, quote = _quote(s, needle, fallback)
    return st.evidence(s.path, ln, quote, extractor)


def _declared(data: dict) -> dict[str, str]:
    out = {}
    for sec in _DEP_SECTIONS:
        for name, ver in (data.get(sec) or {}).items():
            out.setdefault(name, str(ver))
    return out


def _dir_of(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""


def check_package_surface(ctx: Ctx):
    _check_npm_exports(ctx)
    _check_tsconfig(ctx)
    _check_typecheck_scripts(ctx)


def _check_npm_exports(ctx: Ctx):
    """Entrypoints declared in package.json `exports`, checked against real importers."""
    st = ctx.store
    importers: list[tuple[Source, int, str]] = []
    for s in ctx.sources.values():
        if not s.text or s.kind in {"binary", "lockfile", "generated", "diagram"}:
            continue
        for ln, spec in _js_specifiers(s.text):
            if _specifier_package(spec):
                importers.append((s, ln, spec))

    for manifest in ctx.sources.values():
        if manifest.name != "package.json" or not manifest.text:
            continue
        directory = _dir_of(manifest.path)
        cid = ctx.comp_dirs.get(directory)
        if not cid:
            continue
        try:
            data = json.loads(manifest.text)
        except json.JSONDecodeError:
            continue
        exports = data.get("exports")
        if not exports:
            continue
        pkg = data.get("name") or ctx.short(cid)
        declared = _declared(data)
        esm = data.get("type") == "module"
        # one file per subpath: prefer a target that is actually in the repo
        chosen: dict[str, tuple[str, Source]] = {}
        for key, target in _flatten_exports(exports):
            if not isinstance(target, str) or not target.startswith("."):
                continue
            rel = _rel_join(directory, target)
            src = ctx.sources.get(rel)
            if src and src.text and _export_key(key) not in chosen:
                chosen[_export_key(key)] = (target, src)
        shapes = {key: _export_bindings(src.text) for key, (_, src) in chosen.items()}
        used_names: set[str] = set()
        for s, ln, spec in importers:
            if _dir_of(s.path) == directory:
                continue
            if _import_key(spec, pkg) is None:
                continue
            names, _ = _named_imports(s.lines[ln - 1] if 0 < ln <= len(s.lines) else "")
            used_names |= names
        for key, (target, src) in chosen.items():
            if key == ".":
                continue
            external = [(s, ln, spec) for s, ln, spec in importers
                        if _import_key(spec, pkg) == key and _dir_of(s.path) != directory]
            if external:
                continue
            names, has_default, cjs = shapes[key]
            defects, evs = [], []
            evs.append(_ev(st, manifest, f'"{target}"', "package-surface"))
            if esm and (cjs or re.search(r"\brequire\s*\(", src.text)) and not re.search(
                    r"(?m)^\s*(import|export)\s", src.text):
                defects.append(f"{src.name} is CommonJS (require / module.exports) inside an ESM package "
                               '("type": "module")')
                if re.search(r"\brequire\s*\(", src.text):
                    evs.append(_ev(st, src, "require(", "package-surface"))
                if "module.exports" in src.text:
                    evs.append(_ev(st, src, "module.exports", "package-surface"))
                evs.append(_ev(st, manifest, '"type"', "package-surface"))
            missing = []
            for ln, spec in _js_specifiers(src.text):
                dep = _specifier_package(spec)
                if dep and dep not in declared and dep != pkg and dep not in missing:
                    missing.append(dep)
                    evs.append(st.evidence(src.path, ln, src.lines[ln - 1], "package-surface"))
            if missing:
                defects.append("it imports " + ", ".join(missing)
                               + ", which is not in dependencies, devDependencies or peerDependencies")
            comments = _frameworks(_comment_text(src.text))
            export_fw = _FRAMEWORKS.get(key.split("/")[-1])
            if export_fw and comments and export_fw not in comments:
                named = ", ".join(sorted(comments))
                defects.append(f"a comment in the file talks about {named}, not {export_fw}")
                for i, line in enumerate(src.lines, 1):
                    if (_frameworks(line) & comments) and (
                            "/*" in line or line.strip().startswith("*") or "//" in line):
                        evs.append(st.evidence(src.path, i, line, "package-surface"))
                        break
            sibling_files = []
            for other_key, (other_ns, _, _) in shapes.items():
                if other_key != key and used_names and used_names <= other_ns:
                    sibling_files.append(chosen[other_key][1].name)
            if used_names and not (used_names <= names):
                how = "assigns module.exports" if cjs and not names else (
                    "exports default" if has_default else "does not export " + ", ".join(sorted(used_names)))
                via = ", ".join(sibling_files) if sibling_files else "other entrypoints"
                defects.append(f"consumers import {{{', '.join(sorted(used_names))}}} from {via}; "
                               f"this file {how}")
                if has_default:
                    evs.append(_ev(st, src, "export default", "package-surface"))
                for sib in sibling_files[:1]:
                    sib_src = next(s for _, s in chosen.values() if s.name == sib)
                    evs.append(_ev(st, sib_src, "export const", "package-surface"))
            if not defects:
                defects.append("nothing in the repository imports it")
            else:
                defects.insert(0, "nothing in the repository imports it")
            shown = key if key.startswith(".") else f"./{key}"
            st.conflict(
                "stale", "unused-package-export", f"{ctx.short(cid)} {shown}",
                f"{pkg} exports {shown} ({src.path}), but " + "; ".join(defects) + ".",
                0.9, evs)


def _tsconfig_targets(data: dict) -> list[str]:
    vals = []
    ext = data.get("extends")
    if isinstance(ext, str):
        vals.append(ext)
    elif isinstance(ext, list):
        vals.extend(v for v in ext if isinstance(v, str))
    for ref in data.get("references") or []:
        if isinstance(ref, dict) and isinstance(ref.get("path"), str):
            vals.append(ref["path"])
    return vals


def _parse_config_json(s: Source) -> dict | None:
    if not s.text or not s.name.endswith(".json"):
        return None
    try:
        data = _loads_jsonc(s.text)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    if not any(k in data for k in ("compilerOptions", "extends", "references")):
        return None
    return data


def _check_tsconfig(ctx: Ctx):
    st = ctx.store
    configs = []
    for s in ctx.sources.values():
        data = _parse_config_json(s)
        if data is not None:
            configs.append((s, data))
    referenced: set[str] = set()
    missing: list[tuple[Source, str, str]] = []
    for s, data in configs:
        base = _dir_of(s.path)
        for raw in _tsconfig_targets(data):
            if not raw.startswith("."):
                continue
            rel = _rel_join(base, raw)
            if rel in ctx.sources or (rel + ".json") in ctx.sources:
                referenced.add(rel if rel in ctx.sources else rel + ".json")
            else:
                missing.append((s, raw, rel))
    for s, raw, rel in missing:
        cid = ctx.owner(s.path)
        directory = _dir_of(s.path)
        manifest = ctx.sources.get(f"{directory}/package.json" if directory else "package.json")
        pkg = None
        if manifest and manifest.text:
            try:
                pkg = json.loads(manifest.text).get("name")
            except json.JSONDecodeError:
                pkg = None
        ev = _ev(st, s, raw, "package-surface")
        where = f"{s.path} ({pkg})" if pkg else s.path
        st.conflict(
            "conflict", "tsconfig-missing-file", f"{ctx.short(cid) if cid else s.path} {s.name}",
            f"{where} points at {raw}, and {rel} is not in the repository.",
            0.95, [ev])

    # Shareable presets are JSON configs sitting next to a package.json, other than
    # the project's own tsconfig*.json. A preset is used when something extends it.
    presets = []  # (source, package name, component id)
    for s, data in configs:
        directory = _dir_of(s.path)
        manifest = ctx.sources.get(f"{directory}/package.json" if directory else "package.json")
        if not manifest or not manifest.text or s.name.startswith("tsconfig") or s.name == "package.json":
            continue
        if "compilerOptions" not in data:
            continue  # turbo.json also has "extends", and it is not a tsconfig preset
        try:
            pkg = json.loads(manifest.text).get("name")
        except json.JSONDecodeError:
            pkg = None
        cid = ctx.comp_dirs.get(directory)
        if pkg and cid:
            presets.append((s, pkg, cid))
    for s, pkg, cid in presets:
        if s.path in referenced:
            continue
        needle = f"{pkg}/{s.name}"
        if any(other.text and needle in other.text and _dir_of(other.path) != _dir_of(s.path)
               for other in ctx.sources.values()):
            continue
        ev = _ev(st, s, '"compilerOptions"' if '"compilerOptions"' in (s.text or "") else '"extends"',
                 "package-surface")
        st.conflict(
            "stale", "unused-tsconfig-preset", f"{ctx.short(cid)} {s.name}",
            f"{s.path} is a TypeScript preset of {pkg}, and no config extends it.",
            0.85, [ev])

    # The preset package other projects actually extend, if there is a clear one.
    popularity: dict[str, int] = {}
    for s, pkg, cid in presets:
        needle = f"{pkg}/"
        popularity[pkg] = sum(1 for other in ctx.sources.values()
                              if other.text and needle in other.text and _dir_of(other.path) != _dir_of(s.path)
                              and other.name.startswith("tsconfig"))
    if not popularity:
        return
    shared = max(popularity, key=popularity.get)
    if popularity[shared] < 2:
        return
    shared_cid = next(cid for _, pkg, cid in presets if pkg == shared)
    shared_dir = next(d for d, c in ctx.comp_dirs.items() if c == shared_cid)
    example = next((other for other in ctx.sources.values()
                    if other.text and f"{shared}/" in other.text and other.name.startswith("tsconfig")
                    and _dir_of(other.path) != shared_dir), None)
    for directory, cid in ctx.comp_dirs.items():
        if directory == shared_dir:
            continue
        ts = ctx.sources.get(f"{directory}/tsconfig.json" if directory else "tsconfig.json")
        if not ts or not ts.text:
            continue
        try:
            data = _loads_jsonc(ts.text)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or data.get("extends"):
            continue
        manifest = ctx.sources.get(f"{directory}/package.json" if directory else "package.json")
        declared = {}
        if manifest and manifest.text:
            try:
                declared = _declared(json.loads(manifest.text))
            except json.JSONDecodeError:
                declared = {}
        evs = [_ev(st, ts, '"compilerOptions"' if '"compilerOptions"' in ts.text else "{", "package-surface")]
        if example:
            evs.append(_ev(st, example, shared, "package-surface"))
        if shared in declared:
            evs.append(_ev(st, manifest, f'"{shared}"', "package-surface"))
            st.conflict(
                "gap", "tsconfig-ignores-shared", ctx.short(cid),
                f"{ctx.short(cid)} depends on {shared}, but {ts.path} does not extend it and inlines its own "
                f"compilerOptions. Other projects extend {shared}.",
                0.9, evs)
        else:
            uses_ts = ("typescript" in declared or any(n.startswith("@typescript/") for n in declared)
                       or (manifest is not None and manifest.text is not None and _COMPILER_RE.search(manifest.text)))
            if not uses_ts:
                continue
            st.conflict(
                "gap", "tsconfig-not-shared", ctx.short(cid),
                f"{ts.path} is a standalone TypeScript config. {ctx.short(cid)} does not depend on {shared}, "
                f"which other projects extend.",
                0.85, evs)


def _check_typecheck_scripts(ctx: Ctx):
    """A project whose typecheck script calls a different binary than the rest.

    The package named `typescript` and packages under `@typescript/` are reported
    separately, so an old `tsc` pin is not described as the whole toolchain.
    """
    st = ctx.store
    rows = []
    for directory, cid in ctx.comp_dirs.items():
        manifest = ctx.sources.get(f"{directory}/package.json" if directory else "package.json")
        if not manifest or not manifest.text:
            continue
        try:
            data = json.loads(manifest.text)
        except json.JSONDecodeError:
            continue
        scripts = data.get("scripts") or {}
        if not isinstance(scripts, dict):
            continue
        ordered = [k for k in ("check-types", "typecheck", "tsc") if k in scripts]
        ordered += [k for k in scripts if k not in ordered]
        found = None
        for name in ordered:
            m = _COMPILER_RE.search(str(scripts[name]))
            if m:
                found = (name, m.group(1), str(scripts[name]))
                break
        if not found:
            continue
        declared = _declared(data)
        rows.append({
            "cid": cid, "manifest": manifest, "script": found[0], "bin": found[1], "cmd": found[2],
            "typescript": declared.get("typescript"),
            "related": {k: v for k, v in declared.items()
                        if k == "typescript" or k.startswith("@typescript/") or "typescript" in v},
        })
    if len(rows) < 2:
        return
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["bin"]] = counts.get(row["bin"], 0) + 1
    majority = max(counts, key=counts.get)
    if counts[majority] < 2:
        return
    majority_ts = {r["typescript"] for r in rows if r["bin"] == majority and r["typescript"]}
    for row in rows:
        if row["bin"] == majority:
            continue
        evs = [_ev(st, row["manifest"], row["cmd"], "package-surface")]
        if row["typescript"]:
            evs.append(_ev(st, row["manifest"], f'"typescript": "{row["typescript"]}"', "package-surface"))
        parts = [f"{ctx.short(row['cid'])} {row['script']} runs `{row['bin']}`"]
        if row["typescript"]:
            parts[0] += f" from the package named typescript ({row['typescript']})"
        others = [r for r in rows if r["bin"] == majority]
        parts.append(f"{len(others)} other TypeScript projects run `{majority}`")
        if len(majority_ts) == 1:
            ver = next(iter(majority_ts))
            sample = next(r for r in others if r["typescript"] == ver)
            evs.append(_ev(st, sample["manifest"], sample["cmd"], "package-surface"))
            evs.append(_ev(st, sample["manifest"], f'"typescript": "{ver}"', "package-surface"))
            parts.append(f"Their typescript dependency is {ver}")
        # Companion @typescript/* pins, including projects that do not run this script.
        aligned = []
        drifted = []
        for name, ver in sorted(row["related"].items()):
            if name == "typescript":
                continue
            holders = []
            for directory, cid in ctx.comp_dirs.items():
                if cid == row["cid"]:
                    continue
                other_manifest = ctx.sources.get(f"{directory}/package.json" if directory else "package.json")
                if not other_manifest or not other_manifest.text:
                    continue
                try:
                    other_declared = _declared(json.loads(other_manifest.text))
                except json.JSONDecodeError:
                    continue
                if name not in other_declared:
                    continue
                if other_declared[name] == ver:
                    holders.append((cid, other_manifest))
                else:
                    drifted.append(name)
            if holders:
                aligned.append((name, ver, holders))
                evs.append(_ev(st, row["manifest"], f'"{name}"', "package-surface"))
                for _, other_manifest in holders:
                    evs.append(_ev(st, other_manifest, f'"{name}"', "package-surface"))
        if aligned:
            bits = [f"{name} {ver} (also in {', '.join(ctx.short(c) for c, _ in holders)})"
                    for name, ver, holders in aligned]
            parts.append("Aligned, not behind: " + "; ".join(bits))
        if row["typescript"] and majority_ts and row["typescript"] not in majority_ts and not drifted:
            parts.append("Only the package named typescript differs, not every TypeScript package in the project")
        st.conflict(
            "conflict", "typecheck-entrypoint", ctx.short(row["cid"]),
            ". ".join(parts) + ".",
            0.9, evs)


# --------------------------------------------------------------------------
# Security rules (generic patterns; the LLM code pass adds context-specific ones)
# --------------------------------------------------------------------------

@dataclass
class Rule:
    id: str
    severity: str
    pattern: re.Pattern
    kinds: set[str]
    description: str
    guidance: str
    confidence: float = 0.7


CODEISH = {"code", "config", "deployment", "ci", "dependency_manifest"}
ALL_TEXT = CODEISH | DOC_KINDS | {"api_spec", "fixture", "other", "ownership", "schema", "test"}

RULES = [
    Rule("hardcoded-secret", "high",
         re.compile(r"(sk-[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{10,}|"
                    r"-----BEGIN [A-Z ]*PRIVATE KEY|(?i:(?:password|passwd|secret|api[_-]?key|access[_-]?token|"
                    r"client[_-]?secret)\s*[:=]\s*[\"'][^\"'\s]{8,}[\"']))"),
         ALL_TEXT, "Credential-like literal committed to the repository",
         "Never inline secrets; read them from a secret store / env var and rotate this one.", 0.75),
    Rule("cors-any-origin", "medium",
         re.compile(r"\bcors\(\s*\)|allow_origins\s*=\s*\[\s*[\"']\*[\"']|Access-Control-Allow-Origin[\"']?\s*[:,]\s*"
                    r"[\"']\*|AllowAnyOrigin\(\)|allowedOrigins\(\s*\"\*\"|origin:\s*[\"']\*[\"']"),
         CODEISH, "CORS allows any origin",
         "Configure an explicit origin allow-list instead of a wildcard.", 0.85),
    Rule("client-asserted-identity", "high",
         re.compile(r"(?:req(?:uest)?\.(?:get|header|headers)|getHeader|headers\.get|Header\.Get)\b[^\n]{0,30}[\"']"
                    r"x-[a-z-]*(?:user|customer|account|tenant|role)(?:-id)?[\"']", re.I),
         {"code"}, "Caller identity/role is read from a client-supplied header (spoofable)",
         "Derive identity from a verified token/session, never from a plain request header.", 0.7),
    Rule("sql-interpolation", "high",
         re.compile(r"`\s*(?:SELECT|INSERT|UPDATE|DELETE)\b[^`]*\$\{|f[\"'](?:SELECT|INSERT|UPDATE|DELETE)\b[^\"']*\{|"
                    r"[\"'](?:SELECT|INSERT|UPDATE|DELETE)\b[^\"']*[\"']\s*(?:\+|%)\s*\w|String\.format\(\s*\"(?:SELECT|"
                    r"INSERT|UPDATE|DELETE)", re.I),
         {"code"}, "SQL built by string interpolation (injection risk)", "Use parameterised queries only.", 0.8),
    Rule("error-detail-leak", "low",
         re.compile(r"(?:error|message|detail)[\"']?\s*:\s*[^,}\n]*\b(?:error|err|e|ex|exc)\.(?:message|stack)\b"
                    r"|traceback\.format_exc\(\)|printStackTrace\(\)"),
         {"code"}, "Internal error details are returned or printed",
         "Return a generic error body; log details server-side with a request id.", 0.6),
    Rule("dynamic-exec", "high",
         re.compile(r"\beval\(|new Function\(|subprocess\.[a-z_]+\([^)]*shell\s*=\s*True|os\.system\(|Runtime\.getRuntime\(\)\.exec"),
         {"code"}, "Dynamic code / shell execution", "Avoid eval/shell; never pass request data to it.", 0.6),
    Rule("sensitive-data-in-logs", "medium",
         re.compile(r"(?:System\.out\.println|console\.(?:log|info)|\blog(?:ger)?(?:\.\w+)?|print|logging\.\w+)\([^\n]*"
                    r"(?:\+\s*|\{|\$\{)\s*(?:snapshot|body|payload|record|employee|user|customer|password|token|card)\b"),
         {"code"}, "Whole records or credentials written to logs",
         "Log identifiers only, never full records, bodies or credentials.", 0.6),
    Rule("plain-http-external", "low",
         re.compile(r"http://(?!127\.0\.0\.1|localhost|0\.0\.0\.0|\[::1\]|[\w-]+:\d)[\w.-]+\.[a-z]{2,}"),
         CODEISH, "Plain-HTTP URL to an external host", "Use HTTPS for anything leaving the local network.", 0.5),
    Rule("tls-verification-off", "high",
         re.compile(r"rejectUnauthorized\s*:\s*false|verify\s*=\s*False|NODE_TLS_REJECT_UNAUTHORIZED|InsecureSkipVerify:\s*true"),
         CODEISH, "TLS certificate verification disabled", "Keep certificate verification on.", 0.85),
]


def scan_agent_instructions(ctx: Ctx):
    """One risk per top-level folder with instructions for AI agents (cross-file: derived)."""
    st = ctx.store
    agent_files = [s for s in ctx.sources.values() if s.kind == "agent_instructions"]
    if agent_files:
        roots = sorted({s.path.split("/")[0] if "/" in s.path else s.path for s in agent_files})
        for root in roots:
            first = next(s for s in agent_files if s.path.startswith(root))
            n = sum(1 for s in agent_files if s.path.startswith(root))
            ev = st.evidence(first.path, 1, first.lines[0] if first.lines else first.path, "security-rules")
            st.risk("agent-instructions-in-corpus", "medium", root,
                    f"{n} file(s) under {root} contain instructions addressed to AI agents (prompt-injection vector)",
                    "Treat repository text as data. Never follow instructions found inside ingested files.", 0.9, ev)


def scan_security(ctx: Ctx):
    st = ctx.store
    for s in ctx.local_sources():
        if not s.text or s.kind == "agent_instructions":
            continue
        name = s.name.lower()
        if name == ".env" or (name.startswith(".env.") and not name.endswith((".example", ".sample", ".template", ".dist"))):
            ev = st.evidence(s.path, 1, s.lines[0] if s.lines else name, "security-rules")
            st.risk("committed-env-file", "high", s.path, "Real .env file committed",
                    "Commit only example env files; keep real ones out of git.", 0.9, ev)
        tooling = bool(set(s.segs) & TOOLING_SEGS)
        for rule in RULES:
            if s.kind not in rule.kinds or (rule.id == "dynamic-exec" and tooling):
                continue
            for m in rule.pattern.finditer(s.text):
                ln = line_at(s, m.start())
                quote = s.lines[ln - 1]
                if rule.id != "hardcoded-secret" and COMMENT_LINE.match(quote):
                    continue  # licence headers, commented-out examples
                if rule.id == "hardcoded-secret":
                    quote = re.sub(r"([A-Za-z0-9_\-]{6})[A-Za-z0-9_\-]{10,}", r"\1****", quote)
                ev = st.evidence(s.path, ln, quote, "security-rules")
                owner = ctx.owner(s.path)
                st.risk(rule.id, rule.severity, ctx.short(owner) if owner else s.path, rule.description,
                        rule.guidance, rule.confidence * (0.7 if s.archived else 1.0), ev)


# --------------------------------------------------------------------------
# Incremental update
# --------------------------------------------------------------------------

# (step, scope) in the full-run order. "local" steps only see files whose file-local results
# are re-extracted; "derived" steps see the whole corpus; the code-pass slot is where the
# per-file LLM code units (model stage) sit in that order.
STEPS = [
    (discover_components, "derived"), (extract_manifests, "local"), (extract_addresses, "derived"),
    (extract_deployment, "derived"), (extract_code, "local"), (extract_specs, "local"),
    (extract_schemas, "derived"), (extract_owners, "derived"), (None, "code-pass"),
    (extract_docs, "local"), (finalize_components, "derived"), (check_endpoints, "derived"),
    (check_deploy_vs_code, "derived"), (check_schema_drift, "derived"), (check_ownership, "derived"),
    (check_package_surface, "derived"), (scan_agent_instructions, "derived"), (scan_security, "local"),
]
CODE_PASS_STEP = 9
# steps that build the shared context (components, aliases, ports) file-local extractors read
CONTEXT_STEPS = (discover_components, extract_addresses, extract_deployment)

# shared context each file-local extractor depends on (besides the file itself)
FAMILY_CONTEXT = {"manifest": ("names",), "code": ("names", "ports"), "spec": ("names", "ports"),
                  "docs": ("names",), "security": ("names",)}
EXIT_INCOMPLETE = 3  # model enrichment requested but not complete (results so far are published)


def families(s: Source) -> list[str]:
    """File-local extractors that process this file (mirrors their filters; a superset is harmless)."""
    fams = []
    if s.text is None:
        return fams
    if s.kind == "dependency_manifest":
        fams.append("manifest")
    if s.kind in {"code", "test"}:
        fams.append("code")
    if s.kind == "api_spec":
        fams.append("spec")
    if s.kind in DOC_KINDS:
        fams.append("docs")
    if s.kind != "agent_instructions":
        fams.append("security")
    return fams


def det_signature() -> str:
    return json.dumps({"extraction_version": EXTRACTION_VERSION, "schema": SCHEMA_VERSION,
                       "pyyaml": yaml is not None}, sort_keys=True)


def short(commit: str | None) -> str:
    return commit[:12] if commit else "-"


@dataclass
class Unit:
    blob: str | None
    signature: str
    context: str | None
    status: str


class Ingestion:
    def __init__(self, db: sqlite3.Connection, snap: gitsnap.Snapshot, args, repo_name: str, label: str):
        self.db, self.snap, self.args = db, snap, args
        self.repo_name, self.label = repo_name, label
        self.ctx: Ctx | None = None
        self.shared: dict[str, str] = {}
        self.published = False
        self.lines: list[tuple[str, str]] = []  # summary
        self.history: tuple | None = None      # (findings before, last run row) of this invocation
        self.run_id: int | None = None
        self.diff: dict | None = None

    # -- state ---------------------------------------------------------------
    def stage(self, name: str) -> dict | None:
        row = self.db.execute("SELECT commit_id, signature, status, detail FROM stage_state WHERE stage = ?",
                              (name,)).fetchone()
        return dict(zip(("commit", "signature", "status", "detail"), row)) if row else None

    def set_stage(self, name: str, status: str, signature: str, detail: str | None = None):
        self.db.execute("INSERT OR REPLACE INTO stage_state VALUES (?,?,?,?,?,?)",
                        (name, self.snap.head, signature, status, detail,
                         time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))

    def units(self) -> dict[str, Unit]:
        return {r[0]: Unit(*r[1:]) for r in self.db.execute(
            "SELECT unit, blob, signature, context, status FROM extraction_unit")}

    def note(self, label: str, text: str):
        self.lines.append((label, text))

    # -- entry -----------------------------------------------------------------
    def run(self) -> int:
        db, snap = self.db, self.snap
        state = schema_state(db)
        det_sig, model_sig = det_signature(), enrich.model_signature()
        want_model = not self.args.no_llm
        det_ok = False
        if state == "current":
            row = db.execute("SELECT value FROM ingest_meta WHERE key = 'repo_identity'").fetchone()
            if row and row[0] != snap.identity:
                origin = db.execute("SELECT value FROM ingest_meta WHERE key = 'repo_remote'").fetchone()
                raise IngestError(f"{self.args.db} was built from another repository "
                                  f"({origin[0] if origin and origin[0] else row[0]}); use a different --db")
            det, model = self.stage("deterministic"), self.stage("model")
            det_ok = bool(det and det["status"] == "complete" and det["commit"] == snap.head
                          and det["signature"] == det_sig)
            model_ok = bool(model and model["status"] == "complete" and model["commit"] == snap.head
                            and model["signature"] == model_sig)
            if det_ok and (model_ok or not want_model):
                self.note("snapshot", f"{short(snap.head)} already ingested; nothing to do (no writes, no model calls)")
                self.note("stages", self.stage_line())
                return 0
        db.execute("PRAGMA journal_mode = WAL")  # readers keep seeing the last committed state
        if det_ok:
            self.note("snapshot", f"{short(snap.head)} (deterministic stage already complete)")
            self.build_context()
        else:
            self.det_phase(state, det_sig)
        self.published = True
        code = self.model_phase() if want_model else 0
        self.note("stages", self.stage_line())
        return code

    def build_context(self, sources: list[Source] | None = None):
        """Shared extraction context (components, aliases, ports) without writing anything."""
        sources = sources if sources is not None else collect(self.snap)
        if self.ctx is None:
            self.ctx = Ctx(Store(self.db), {s.path: s for s in sources}, repo_name=self.repo_name,
                           repo=self.snap.repo)
        ctx = self.ctx
        ctx.reset()
        ctx.store.use(None)
        for step in CONTEXT_STEPS:
            step(ctx)
        self.shared = {
            "names": digest([self.repo_name, sorted(ctx.comp_dirs.items()),
                             sorted((k, v or "") for k, v in ctx.aliases.items()), sorted(ctx.archived_comps)]),
            "ports": digest(sorted((p, sorted(c)) for p, c in ctx.ports.items())),
        }

    def code_context(self) -> str:
        """What an LLM code unit's stored result depends on besides its file (target resolution)."""
        return digest([self.shared["names"], self.shared["ports"], det_signature()])

    def run_steps(self, fact_units: list[str], derived: bool):
        """Run the pipeline in full-run order: re-extract ctx.regen, recompute cross-source results."""
        ctx = self.ctx
        st = ctx.store
        if derived:
            ctx.reset()
            ctx.load_facts(fact_units)
        for i, (step, scope) in enumerate(STEPS, 1):
            st.step = i
            if step is None:
                continue  # code pass: model stage
            if scope == "local":
                if ctx.regen:
                    step(ctx)
            elif derived:
                if step is finalize_components:
                    ctx.sort_facts()
                st.use(DERIVED)
                step(ctx)

    # -- deterministic stage ---------------------------------------------------
    def det_phase(self, state: str, det_sig: str):
        db, snap = self.db, self.snap
        sources = collect(snap)  # reads Git objects only, before the transaction
        with transaction(db):
            self.capture_history()
            mode = "incremental update"
            if state == "empty":
                create_schema(db)
                mode = "initial build"
            elif state == "legacy":
                migrate_legacy(db)
                mode = "legacy schema migrated to v2 (one-time full refresh)"
            det = self.stage("deterministic")
            prev = det["commit"] if det else None
            if det and det["signature"] != det_sig:
                mode = f"extraction signature changed ({det['signature']} -> {det_sig}): every file re-extracted"
            st = Store(db)
            ctx = self.ctx = Ctx(st, {s.path: s for s in sources}, repo_name=self.repo_name, repo=snap.repo)
            model_files = {u for u in self.units() if u.startswith("llm:") and u not in (DIAGRAM_CHECK, VERIFY)}

            # 1. which files changed (Git diff between the stored and the current commit)
            old = {r[0]: r[1:] for r in db.execute(
                "SELECT path, blob, det_kind, archived, bytes, git_date FROM source")}
            added, modified, deleted = self.changed_files(prev, old)
            self.note("snapshot", f"{short(prev)} -> {short(snap.head)}  [{mode}]")
            self.note("files", f"{len(added)} added, {len(modified)} modified, {len(deleted)} deleted, "
                               f"{len(snap.files) - len(added) - len(modified)} unchanged")
            for path in deleted:
                db.execute("DELETE FROM source WHERE path = ?", (path,))
                st.set_fts(path, "", None)
                for unit in (f"det:{path}", f"{CODE_UNIT}{path}", f"llm:{path}"):
                    st.drop(unit)
            dates_changed = False
            for s in sources:
                dates_changed |= self.write_source(st, s, old.get(s.path))

            # 2. which file-local units must be re-extracted
            self.build_context(sources)
            units = self.units()
            reasons: Counter = Counter()
            expected: dict[str, str] = {}
            unprocessed = 0
            for s in sources:
                unit, fams = f"det:{s.path}", families(s)
                if not fams:
                    unprocessed += 1
                    if unit in units:
                        st.drop(unit)
                    continue
                expected[s.path] = digest({f: [self.shared[k] for k in FAMILY_CONTEXT[f]] for f in fams})
                row = units.get(unit)
                reason = ("new" if row is None else "content" if row.blob != s.blob
                          else "version" if row.signature != det_sig
                          else "shared context" if row.context != expected[s.path] else None)
                if reason:
                    ctx.regen.add(s.path)
                    reasons[reason] += 1
            # LLM code units feed the cross-source checks: drop the ones that no longer match first
            code_dropped = self.invalidate_code_units(st)
            text_changed = bool(added or deleted) or any(
                ctx.sources[p].text is not None or old[p][1] not in {"diagram", "binary", "lockfile"} for p in modified)
            derived = (bool(ctx.regen) or text_changed or dates_changed or bool(code_dropped)
                       or DERIVED not in units or units[DERIVED].signature != det_sig)

            # 3. extraction: re-extract changed units, recompute cross-source results
            self.run_steps([f"det:{p}" for p in expected if p not in ctx.regen] + self.code_units(), derived)
            st.finish()
            for path in ctx.regen:
                st.mark(f"det:{path}", "deterministic", snap.head, det_sig, path=path,
                        blob=ctx.sources[path].blob, context=expected[path])
            if derived:
                st.mark(DERIVED, "deterministic", snap.head, det_sig)
            st.materialize()
            regen = ", ".join(f"{n} {r}" for r, n in reasons.most_common())
            self.note("rules", f"{len(ctx.regen)} files re-extracted" + (f" ({regen})" if regen else "")
                      + f", {len(expected) - len(ctx.regen)} reused, {unprocessed} not processed by rules")
            self.note("cross-source", ("checks recomputed" if derived else
                                       "checks not needed (no input changed)")
                      + "; " + self.record_line(st.stats, ("file", "derived")))

            # 4. model-stage results that no longer match this snapshot
            self.invalidate_model(st)
            dropped = len(model_files - self.units().keys())
            mutated = st.mutations
            if mutated or DIAGRAM_CHECK not in units:
                self.check_diagrams(st)
            reset = bool(st.mutations) and (VERIFY in units or DIAGRAM_CHECK in units)
            if st.mutations:
                self.reset_verification(st)
            st.materialize()
            if dropped or reset:
                self.note("model data", f"{dropped} file results invalidated"
                          + ("; verification reset (knowledge changed)" if reset else ""))

            self.publish_checks()
            db.execute("INSERT OR REPLACE INTO ingest_meta VALUES ('repo_identity', ?), ('repo_remote', ?)",
                       (snap.identity, snap.remote))
            self.set_stage("deterministic", "complete", det_sig)
            self.update_model_stage()
            self.record_run()

    def changed_files(self, prev: str | None, old: dict) -> tuple[list[str], list[str], list[str]]:
        snap = self.snap
        cur = snap.files
        by_blob = ([p for p in cur if p not in old], [p for p in cur if p in old and old[p][0] != cur[p]],
                   [p for p in old if p not in cur])
        if not prev or not old:
            return by_blob
        if prev == snap.head:
            changes, how = {}, "same commit"
        elif gitsnap.has_commit(snap.repo, prev):
            changes, how = gitsnap.diff_paths(snap.repo, prev, snap.head), f"git diff {short(prev)}..{short(snap.head)}"
        else:
            self.note("diff", f"stored commit {short(prev)} is not in this checkout's history; compared stored blob ids")
            return by_blob
        added = [p for p in changes if p in cur and p not in old]
        modified = [p for p in changes if p in cur and p in old and old[p][0] != cur[p]]
        deleted = [p for p in changes if p in old and p not in cur]
        # blob ids are authoritative: anything the diff missed (e.g. an interrupted older run) is resynced
        extra = [set(b) - set(a) for a, b in zip((added, modified, deleted), by_blob)]
        n_extra = sum(len(e) for e in extra)
        self.note("diff", how + (f"; {n_extra} more files differed from stored blob ids" if n_extra else ""))
        return tuple(a + sorted(e) for a, e in zip((added, modified, deleted), extra))

    def write_source(self, st: Store, s: Source, old: tuple | None) -> bool:
        """Insert/refresh one source row; returns True if only a text file's git date moved."""
        if old is None:
            self.db.execute("INSERT INTO source (path, kind, det_kind, archived, doc_date, git_date, bytes, text, blob) "
                            "VALUES (?,?,?,?,?,?,?,?,?)",
                            (s.path, s.kind, s.kind, int(s.archived), s.doc_date, s.git_date, s.size, s.text, s.blob))
            st.set_fts(s.path, s.kind, s.text)
            return False
        blob, det_kind, archived, size, git_date = old
        if (blob, det_kind, archived, size) != (s.blob, s.kind, int(s.archived), s.size):
            self.db.execute("UPDATE source SET kind = ?, det_kind = ?, archived = ?, doc_date = ?, git_date = ?, "
                            "bytes = ?, blob = ?, doc_status = NULL, summary = NULL WHERE path = ?",
                            (s.kind, s.kind, int(s.archived), s.doc_date, s.git_date, s.size, s.blob, s.path))
            st.set_text(s.path, s.kind, s.text)
            for unit in (f"{CODE_UNIT}{s.path}", f"llm:{s.path}"):
                st.drop(unit)  # model output describes the previous contents
            return False
        if git_date != s.git_date:
            self.db.execute("UPDATE source SET git_date = ? WHERE path = ?", (s.git_date, s.path))
            return s.text is not None
        return False

    # -- model stage -------------------------------------------------------------
    def model_units(self) -> dict[str, tuple[Source, str]]:
        ctx = self.ctx
        units = {f"{CODE_UNIT}{s.path}": (s, "code") for s in enrich.code_sources(ctx)}
        units.update({f"llm:{s.path}": (s, "prose") for s in enrich.prose_sources(ctx)})
        units.update({f"llm:{s.path}": (s, "diagram") for s in enrich.diagram_sources(ctx)})
        return units

    def code_units(self) -> list[str]:
        """Complete LLM code units whose facts take part in the cross-source checks."""
        return [u for u, row in self.units().items() if u.startswith(CODE_UNIT) and row.status == "complete"]

    def unit_context(self, kind: str, catalog: str | None = None) -> str:
        return self.code_context() if kind == "code" else digest(catalog if catalog is not None
                                                                 else enrich.catalog_text(self.ctx))

    def invalidate_code_units(self, st: Store) -> int:
        want = {u: s for u, (s, kind) in self.model_units().items() if kind == "code"}
        context, dropped = self.code_context(), 0
        for unit, row in self.units().items():
            if unit.startswith(CODE_UNIT) and (unit not in want or row.blob != want[unit].blob
                                               or row.context != context):
                st.drop(unit)
                dropped += 1
        return dropped

    def invalidate_model(self, st: Store):
        """Drop prose/diagram results whose file, blob or component catalog no longer match (any mode)."""
        catalog = digest(enrich.catalog_text(self.ctx))
        want = self.model_units()
        for unit, row in self.units().items():
            if not unit.startswith("llm:") or unit.startswith(CODE_UNIT) or unit in (DIAGRAM_CHECK, VERIFY):
                continue
            if unit not in want or row.blob != want[unit][0].blob or row.context != catalog:
                st.drop(unit)
                if unit[4:] in self.ctx.sources:
                    self.reset_model_columns(st, self.ctx.sources[unit[4:]])

    def reset_model_columns(self, st: Store, s: Source):
        self.db.execute("UPDATE source SET kind = det_kind, doc_status = NULL, summary = NULL, doc_date = ? "
                        "WHERE path = ?", (s.doc_date, s.path))
        st.set_text(s.path, s.kind, s.text)  # diagram description removed; FTS kind back to the rule-based one

    def check_diagrams(self, st: Store):
        st.use(DIAGRAM_CHECK)
        enrich.check_diagrams(self.ctx)
        st.finish()
        st.mark(DIAGRAM_CHECK, "model", self.snap.head, "diagram edges vs observed relations")

    def reset_verification(self, st: Store):
        st.drop(VERIFY)
        self.db.execute("UPDATE conflict SET verdict = NULL, rationale = NULL, confidence = base_confidence "
                        "WHERE verdict IN ('confirmed', 'rejected')")

    def model_pending(self) -> list[str]:
        sigs = enrich.signatures()
        catalog = digest(enrich.catalog_text(self.ctx))
        units = self.units()
        pending = []
        for u, (s, kind) in self.model_units().items():
            row = units.get(u)
            context = self.code_context() if kind == "code" else catalog
            if (row is None or row.status != "complete" or row.blob != s.blob or row.signature != sigs[kind]
                    or row.context != context):
                pending.append(u)
        for u, sig in ((DIAGRAM_CHECK, None), (VERIFY, sigs["verify"])):
            row = units.get(u)
            if row is None or row.status != "complete" or (sig and row.signature != sig):
                pending.append(u)
        return pending

    def update_model_stage(self, failures: str | None = None):
        pending = self.model_pending()
        if not pending:
            self.set_stage("model", "complete", enrich.model_signature())
            return
        files = sum(1 for u in pending if u not in (DIAGRAM_CHECK, VERIFY))
        why = failures or ("not requested (--no-llm)" if self.args.no_llm else "not run")
        self.set_stage("model", "incomplete", enrich.model_signature(),
                       f"{why}; pending: {files} files" + (", verification" if VERIFY in pending else ""))

    def model_phase(self) -> int:
        db, ctx, snap = self.db, self.ctx, self.snap
        if not self.model_pending():
            return 0
        if not llm.available():
            with transaction(db):
                self.update_model_stage("credentials missing: OPENROUTER_API_KEY is not set (environment or .env)")
                if self.run_id is not None:  # this invocation already published a deterministic update
                    self.record_run()
            self.note("model", "NOT RUN - OPENROUTER_API_KEY is not set (environment or .env); deterministic "
                               "results are published, model stage stays incomplete")
            return EXIT_INCOMPLETE
        sigs = enrich.signatures()
        workers = self.args.workers
        print(f"model stage (OpenRouter): extract={llm.EXTRACT_MODEL} reason={llm.REASON_MODEL}")
        with transaction(db):
            self.capture_history()
            st = ctx.store = Store(db)
            ctx.regen = set()
            failed_n = 0

            # A. code pass (any language): its endpoints/calls feed the cross-source checks
            pending = self.model_pending()
            todo = {u: v for u, v in self.model_units().items() if u in pending and v[1] == "code"}
            st.step = CODE_PASS_STEP
            ok, failed = enrich.code_pass(ctx, workers, [s for s, _ in todo.values()])
            st.finish()
            for path in ok:
                st.mark(f"{CODE_UNIT}{path}", "model", snap.head, sigs["code"], path=path,
                        blob=ctx.sources[path].blob, context=self.code_context())
            for path in failed:
                st.drop(f"{CODE_UNIT}{path}")
            failed_n += len(failed)
            if self.args.show_unresolved:
                for path, hint in sorted(set(ctx.unresolved)):
                    print(f"    unresolved call target {hint!r} in {path}")
            code_changed = st.mutations
            if code_changed:  # same checks as a full run that had these code-pass results
                det_units = [u for u in self.units() if u.startswith("det:") and u != DERIVED]
                self.run_steps(det_units + self.code_units(), derived=True)
                st.finish()
                st.mark(DERIVED, "deterministic", snap.head, det_signature())
            st.materialize()
            enriched = len(ok)

            # B. prose and diagrams (their prompt carries the component catalog)
            pending = self.model_pending()
            todo = {u: v for u, v in self.model_units().items() if u in pending and v[1] != "code"}
            for s, _ in todo.values():
                self.reset_model_columns(st, s)
            catalog = enrich.catalog_text(ctx)
            ok, failed = enrich.extract_prose(ctx, catalog, workers, [s for s, k in todo.values() if k == "prose"])
            ok2, failed2 = enrich.read_diagrams(ctx, catalog, workers, [s for s, k in todo.values() if k == "diagram"])
            st.finish()
            for path in ok + ok2:
                s, kind = todo[f"llm:{path}"]
                st.mark(f"llm:{path}", "model", snap.head, sigs[kind], path=path, blob=s.blob, context=digest(catalog))
            for path in failed + failed2:
                st.drop(f"llm:{path}")
                self.reset_model_columns(st, ctx.sources[path])
            st.materialize()
            enriched += len(ok) + len(ok2)
            failed_n += len(failed) + len(failed2)

            # C. diagram-vs-code comparison and verification
            changed = st.mutations
            if changed or DIAGRAM_CHECK in pending:
                self.check_diagrams(st)
            verify_failed = 0
            rerun = bool(changed) or VERIFY in pending
            if rerun:
                self.db.execute("UPDATE conflict SET verdict = NULL, rationale = NULL, confidence = base_confidence "
                                "WHERE verdict IN ('confirmed', 'rejected')")
                st.use(VERIFY)
                verify_failed = enrich.verify(ctx, workers)
                st.finish()
                st.mark(VERIFY, "model", snap.head, sigs["verify"], status="partial" if verify_failed else "complete")
            st.materialize()
            self.publish_checks()
            failures = (f"{failed_n} files and {verify_failed} verification jobs failed"
                        if failed_n or verify_failed else None)
            self.update_model_stage(failures)
            self.record_run()
        reused = len(self.model_units()) - enriched - failed_n
        self.note("model", f"{enriched} files enriched, {reused} reused, {failed_n} failed; "
                           f"cross-source checks {'recomputed' if code_changed else 'unchanged'}; "
                           f"verification {'re-run' if rerun else 'reused'}"
                           + (f" ({verify_failed} failed)" if verify_failed else "")
                           + "; " + self.record_line(st.stats, ("model",)))
        return EXIT_INCOMPLETE if failures else 0

    # -- history -------------------------------------------------------------------
    def capture_history(self):
        """Active findings and last run before this invocation's first write (the comparison baseline)."""
        if self.history is not None:
            return
        db = self.db
        tables = _tables(db)
        before = last = None
        if "run" in tables and "fingerprint" in _columns(db, "conflict") and "fingerprint" in _columns(db, "risk"):
            last = db.execute("SELECT * FROM run ORDER BY id DESC LIMIT 1").fetchone()
            before = active_findings(db)
        self.history = (before, last)

    def record_run(self):
        """One run row per invocation; compared only with a run of the same repo and model-stage state."""
        db = self.db
        before, last = self.history
        fingerprint_findings(db)
        now = active_findings(db)
        model = self.stage("model")
        complete = bool(model and model["status"] == "complete")
        comparable = bool(before is not None and last and last[1] == self.label and bool(last[4]) == complete)
        new = {fp: v for fp, v in now.items() if comparable and fp not in before}
        resolved = {fp: v for fp, v in (before.items() if comparable else []) if fp not in now}
        n_conf = sum(1 for v in now.values() if v[0] == "conflict")
        row = (self.label, self.snap.head[:7], datetime.now().isoformat(timespec="seconds"), int(complete), n_conf,
               len(now) - n_conf, len(new), len(resolved), last[0] if comparable else None)
        if self.run_id is None:
            self.run_id = db.execute("INSERT INTO run (repo, commit_sha, finished, llm, conflicts, risks, new, "
                                     "resolved, baseline_run) VALUES (?,?,?,?,?,?,?,?,?)", row).lastrowid
        else:
            db.execute("UPDATE run SET repo = ?, commit_sha = ?, finished = ?, llm = ?, conflicts = ?, risks = ?, "
                       "new = ?, resolved = ?, baseline_run = ? WHERE id = ?", (*row, self.run_id))
        db.execute("DELETE FROM change WHERE run_id = ?", (self.run_id,))
        db.executemany("INSERT INTO change VALUES (?,?,?,?,?,?)",
                       [(self.run_id, v[0], "new", fp, v[1], v[2]) for fp, v in new.items()] +
                       [(self.run_id, v[0], "resolved", fp, v[1], None) for fp, v in resolved.items()])
        self.diff = {"run": self.run_id, "baseline": last if comparable else None, "had_previous": bool(last),
                     "new": new, "resolved": resolved}

    # -- publication ---------------------------------------------------------------
    def publish_checks(self):
        problems = integrity_problems(self.db)
        if problems:
            raise IngestError("integrity check failed, nothing was published: " + "; ".join(problems))

    @staticmethod
    def record_line(stats: Counter, groups) -> str:
        parts = []
        for g in groups:
            a, r, k = stats[g, "added"], stats[g, "removed"], stats[g, "kept"]
            label = {"file": "file-local", "derived": "cross-source", "model": "model"}[g]
            parts.append(f"{label} records +{a} -{r} ={k}")
        return ", ".join(parts)

    def stage_line(self) -> str:
        out = []
        for name in ("deterministic", "model"):
            st = self.stage(name)
            if not st:
                out.append(f"{name} never run")
                continue
            out.append(f"{name} {st['status']} @{short(st['commit'])}" + (f" ({st['detail']})" if st["detail"] else ""))
        return "; ".join(out)

    def print_summary(self):
        snap, db = self.snap, self.db
        print(f"{'source repo':<13}: {snap.repo}" + (f"  (origin {snap.remote})" if snap.remote else ""))
        print(f"{'database':<13}: {self.args.db}")
        for label, text in self.lines:
            print(f"{label:<13}: {text}")
        if schema_state(db) != "current":
            return
        print(f"{'components':<13}: " + ", ".join(f"{k} {n}" for k, n in db.execute(
            "SELECT kind, COUNT(*) FROM component WHERE id LIKE 'comp:%' GROUP BY kind")))
        counts = ", ".join(f"{t} {db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]}"
                           for t in ("source", "component", "relation", "evidence", "claim", "conflict", "risk"))
        print(f"{'totals':<13}: {counts}")
        if self.args.stats:
            print("files by kind:")
            for kind, n in db.execute("SELECT kind, COUNT(*) FROM source GROUP BY kind ORDER BY 2 DESC"):
                print(f"  {kind:<20}{n}")
            print("conflicts by check:")
            for chk, v, n in db.execute("SELECT check_name, IFNULL(verdict, '-'), COUNT(*) FROM conflict "
                                        "GROUP BY 1, 2 ORDER BY 3 DESC"):
                print(f"  {chk:<32}{v:<11}{n}")
            print("risks by rule:")
            for rule, sev, n in db.execute("SELECT rule, severity, COUNT(*) FROM risk GROUP BY 1,2 ORDER BY 3 DESC"):
                print(f"  {rule:<32}{sev:<8}{n}")
        if self.diff:
            print_diff(self.diff, short(snap.head))


def print_diff(diff: dict, commit: str):
    base = diff["baseline"]
    print(f"\nrun #{diff['run']} at {commit}", end="")
    if not base:
        print(" - no comparable previous run" + (" (different repo or model-stage state)" if diff["had_previous"]
                                                 else "") + "; this run is the new baseline")
        return
    print(f", compared with run #{base[0]} at {base[2]} ({base[3]}):")
    for change, items in (("NEW", diff["new"]), ("RESOLVED", diff["resolved"])):
        conf = sum(1 for v in items.values() if v[0] == "conflict")
        print(f"  {change:<9} {conf} conflicts, {len(items) - conf} risks")
        for item, summary, _ in sorted(items.values(), key=lambda v: (v[0], v[1]))[:10]:
            print(f"    {'+' if change == 'NEW' else '-'} {item}: {summary[:150]}")
        if len(items) > 10:
            print(f"    ... {len(items) - 10} more (python3 query.py changes)")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-url", default=DEFAULT_REPO_URL)
    ap.add_argument("--repo", type=Path, help="existing local checkout (read-only, must have no tracked changes)")
    ap.add_argument("--db", type=Path, default=HERE / "context.db")
    ap.add_argument("--no-llm", action="store_true", help="skip the OpenRouter stages")
    ap.add_argument("--workers", type=int, default=8, help="parallel LLM requests")
    ap.add_argument("--show-unresolved", action="store_true", help="list call targets no component matched")
    ap.add_argument("--label", help="name of the source for run-to-run comparison (default: repo url/path)")
    ap.add_argument("--stats", action="store_true", help="also print per-kind/check/rule breakdowns")
    return ap.parse_args(argv)


def run(argv=None) -> int:
    """0 = requested stages complete, 1 = error (database untouched), 3 = model stage incomplete."""
    args = parse_args(argv)
    if yaml is None:
        print("warning: PyYAML not installed - compose/Kubernetes/YAML specs are skipped (pip install pyyaml)")
    try:
        repo = gitsnap.toplevel(prepare_repo(args.repo_url, args.repo))
        dirty = gitsnap.dirty_tracked(repo)
        if dirty:
            listing = ", ".join(d.strip() for d in dirty[:5]) + (", ..." if len(dirty) > 5 else "")
            raise IngestError(
                f"{repo} has uncommitted changes to tracked files ({listing}). The database records a "
                "commit, so only committed content can be ingested: commit or stash them (git stash), or use "
                "a clean clone. Untracked files are ignored. The database was not modified.")
        snap = gitsnap.snapshot(repo)
    except (IngestError, gitsnap.GitError, subprocess.CalledProcessError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    name = (args.repo.name if args.repo else args.repo_url.rstrip("/").split("/")[-1]).removesuffix(".git").lower()
    label = args.label or (str(args.repo.resolve()) if args.repo else args.repo_url)
    created = not args.db.exists()
    db = sqlite3.connect(args.db, isolation_level=None, timeout=30)
    ing = Ingestion(db, snap, args, name, label)
    try:
        code = ing.run()
        ing.print_summary()
        return code
    except IngestError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except sqlite3.OperationalError as e:
        if "locked" not in str(e):
            raise
        print(f"error: {args.db} is locked by another ingestion; nothing was changed ({e})", file=sys.stderr)
        return 1
    finally:
        db.close()
        if created and not ing.published:  # never leave an empty, half-created database behind
            for suffix in ("", "-wal", "-shm"):
                Path(str(args.db) + suffix).unlink(missing_ok=True)


def main():
    return run()


if __name__ == "__main__":
    sys.exit(main())
