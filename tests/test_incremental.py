"""Incremental, Git-driven ingestion: temporary Git repositories and databases, mocked model calls.

    python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import ask  # noqa: E402
import enrich  # noqa: E402
import ingest  # noqa: E402
import llm  # noqa: E402
import query  # noqa: E402

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 20  # >= 4 KB, or it is classified as an icon

CORPUS = {
    "apps/order-service/package.json": json.dumps({
        "name": "@ns/order-service",
        "dependencies": {"express": "^4.19.0", "@ns/shared": "workspace:*"}}, indent=2),
    "apps/order-service/src/server.js": """const express = require("express");
const app = express();
const users = process.env.USER_SERVICE_URL ?? "http://127.0.0.1:5002";

app.get("/v1/orders/:id", async (req, res) => {
  const user = await fetch(`${users}/v1/users/${req.params.customer}`);
  const me = await fetch("http://127.0.0.1:5002/v1/users/me");
  const row = db.prepare("SELECT id, total_minor FROM orders WHERE id = ?").get(req.params.id);
  res.json(row);
});

app.post("/v1/orders", (req, res) => res.status(201).end());
""",
    "apps/order-service/migrations/001-orders.sql":
        "CREATE TABLE orders (\n  id TEXT PRIMARY KEY,\n  total REAL NOT NULL\n);\n",
    "apps/order-service/migrations/002-minor-units.sql":
        "ALTER TABLE orders ADD COLUMN total_minor INTEGER;\nALTER TABLE orders DROP COLUMN total;\n",
    "apps/user-service/package.json": json.dumps({
        "name": "@ns/user-service", "dependencies": {"express": "^4.19.0", "cors": "^2.8.5"}}, indent=2),
    "apps/user-service/src/server.js": """const express = require("express");
const cors = require("cors");
const app = express();
app.use(cors());
app.get("/v1/users/:id", (req, res) => res.json({ id: req.params.id }));
""",
    "packages/shared/package.json": json.dumps({"name": "@ns/shared"}, indent=2),
    "packages/shared/index.js": "module.exports = { money: (x) => x };\n",
    "config/ports.json": json.dumps({"order-service": 5001, "user-service": 5002}, indent=2),
    "compose.yaml": """services:
  order-service:
    depends_on: [user-service]
    environment:
      USER_SERVICE_URL: "http://user-service:5002"
  user-service:
    image: user-service
""",
    ".github/CODEOWNERS": "/apps/order-service @team-orders\n",
    "api-specs/order-service.json": json.dumps({
        "openapi": "3.0.0", "info": {"version": "1.0"},
        "servers": [{"url": "http://127.0.0.1:5001"}],
        "paths": {"/v1/orders/{id}": {"get": {}}, "/v1/orders": {"post": {}},
                  "/v1/orders/{id}/refund": {"post": {}}}}, indent=2),
    "wiki/orders.md": """# Orders

The order-service exposes GET /v1/orders/{id} for the storefront.
Order responses expose the `total` field as a decimal number of dollars.
Search marker: zebracorn.
""",
    "docs/notes.md": "# Notes\n\nThe order-service calls user-service for customer names on every request.\n",
    "diagrams/architecture.png": PNG,
    "pnpm-lock.yaml": "lockfileVersion: '9.0'\n",
}


# the first, rebuild-per-run schema (no fingerprints, no run history)
LEGACY_V1_SCHEMA = """
CREATE TABLE source (path TEXT PRIMARY KEY, kind TEXT NOT NULL, archived INTEGER NOT NULL, doc_date TEXT,
    git_date TEXT, bytes INTEGER NOT NULL, text TEXT, doc_status TEXT, summary TEXT);
CREATE TABLE component (id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, path TEXT, props TEXT);
CREATE TABLE evidence (id INTEGER PRIMARY KEY, path TEXT NOT NULL, line INTEGER, quote TEXT NOT NULL,
    extractor TEXT NOT NULL);
CREATE TABLE relation (id INTEGER PRIMARY KEY, src TEXT NOT NULL, type TEXT NOT NULL, dst TEXT NOT NULL,
    status TEXT NOT NULL, confidence REAL NOT NULL, evidence_id INTEGER NOT NULL, props TEXT);
CREATE TABLE conflict (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, check_name TEXT NOT NULL, subject TEXT NOT NULL,
    description TEXT NOT NULL, confidence REAL NOT NULL, evidence_ids TEXT NOT NULL, verdict TEXT, rationale TEXT);
CREATE TABLE claim (id INTEGER PRIMARY KEY, component TEXT NOT NULL, topic TEXT, text TEXT NOT NULL,
    path TEXT NOT NULL, doc_date TEXT, doc_status TEXT, evidence_id INTEGER NOT NULL);
CREATE TABLE risk (id INTEGER PRIMARY KEY, rule TEXT NOT NULL, severity TEXT NOT NULL, subject TEXT,
    description TEXT NOT NULL, guidance TEXT NOT NULL, confidence REAL NOT NULL, evidence_id INTEGER NOT NULL);
CREATE INDEX relation_src ON relation(src);
CREATE INDEX relation_dst ON relation(dst);
"""
SECRET = 'const apiKey = "sk-live-0123456789abcdefghijklmnop";\n'


# ----------------------------------------------------------------------------- fake model

class FakeModel:
    """Deterministic stand-in for llm.chat (results depend on content, never on row ids)."""

    def __init__(self):
        self.calls: Counter = Counter()
        self.lock = threading.Lock()
        self.fail = False

    def __call__(self, messages, model, *, tools=None, json_mode=False, max_tokens=4000, cache=True,
                 tool_choice=None):
        system, user = messages[0]["content"], messages[1]["content"]
        stage = ("prose" if system == enrich.EXTRACT_SYSTEM else "diagram" if system == enrich.DIAGRAM_SYSTEM
                 else "verify" if system == enrich.VERIFY_SYSTEM else "code" if system == enrich.CODE_SYSTEM
                 else "other")
        with self.lock:
            self.calls[stage] += 1
        if self.fail:
            raise llm.LLMError("simulated OpenRouter outage")
        return {"content": json.dumps(getattr(self, stage)(user))}

    @staticmethod
    def prose(user):
        path = re.search(r"Document path: (\S+)", user).group(1)
        doc = user.split("<document>\n", 1)[1].rsplit("\n</document>", 1)[0]
        line = next(l.strip() for l in doc.splitlines() if len(l.strip()) >= 12)
        stem = Path(path).stem
        return {"doc_type": "wiki" if "orders" in path else "other",
                "doc_status": "current", "effective_date": None, "summary": f"summary of {path}",
                "facts": [{"subject": "order-service", "relation": "DEPENDS_ON", "object": f"topic {stem}",
                           "quote": line, "confidence": 0.9}],
                "claims": [{"component": "order-service", "topic": "api", "claim": f"{stem}: {line}",
                            "quote": line}],
                "risks": []}

    @staticmethod
    def diagram(user):
        return {"title": "architecture", "diagram_type": "architecture", "date_or_version": None,
                "nodes": [{"name": "Orders", "maps_to": "comp:order-service"},
                          {"name": "Users", "maps_to": "comp:user-service"},
                          {"name": "Billing", "maps_to": None}],
                "edges": [{"from": "Orders", "to": "Billing", "label": "charges"}],
                "notes": ["zebradiagram marker"]}

    @staticmethod
    def code(user):
        path = re.search(r"File: (\S+)", user).group(1)
        text = user.split("<file>\n", 1)[1]
        out = {"routes": [], "calls": [], "sql": [], "events": [], "risks": []}
        post = next((l for l in text.splitlines() if "app.post(" in l), None)
        fetch = next((l for l in text.splitlines() if "${users}" in l), None)
        if "order-service" in path and post:
            # a route the regex does not see, and a call it already found (must not be duplicated)
            out["routes"].append({"method": "DELETE", "path": "/v1/orders/{id}", "quote": post})
            out["risks"].append({"description": "no authorization check", "severity": "high",
                                 "guidance": "check the caller", "quote": post})
        if "order-service" in path and fetch:
            out["calls"].append({"target": "USER_SERVICE_URL", "method": "GET", "path": "/v1/users/{id}",
                                 "quote": fetch})
        return out

    @staticmethod
    def verify(user):
        cands = re.findall(r"^  C(\d+) (.*)$", user, re.M)
        evs = re.findall(r"^\s+E(\d+) (.*)$", user, re.M)
        out = {"candidate_reviews": [], "contradictions": []}
        if cands:
            cid = min(cands, key=lambda c: c[1])[0]
            out["candidate_reviews"].append({"conflict_id": f"C{cid}", "verdict": "confirmed", "rationale": "fake"})
        if evs:
            ev = min(evs, key=lambda e: e[1])[0]
            out["contradictions"].append({"kind": "stale", "description": "fake contradiction",
                                          "evidence_ids": [f"E{ev}"], "current_evidence_id": f"E{ev}",
                                          "why_current": "fake", "confidence": 0.5})
        return out


# ----------------------------------------------------------------------------- helpers

class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ctx-test-"))
        self.repo = self.tmp / "repo"
        self.db = self.tmp / "context.db"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "main")
        self.write(CORPUS)
        self.commit("initial")
        self.fake = FakeModel()
        patches = [mock.patch.object(llm, "chat", self.fake),
                   mock.patch.object(llm, "available", lambda: True),
                   mock.patch.object(llm, "CACHE", self.tmp / "llm-cache"),
                   mock.patch.object(llm, "EXTRACT_MODEL", "test/extract-model"),
                   mock.patch.object(llm, "REASON_MODEL", "test/reason-model")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def git(self, *args) -> str:
        env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.test",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.test",
               "GIT_AUTHOR_DATE": "2026-03-01T10:00:00", "GIT_COMMITTER_DATE": "2026-03-01T10:00:00"}
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True,
                              text=True, env=env).stdout

    def write(self, files: dict):
        for rel, content in files.items():
            p = self.repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(content if isinstance(content, bytes) else content.encode())

    def commit(self, msg: str, *, add=True) -> str:
        if add:
            self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", msg)
        return self.git("rev-parse", "HEAD").strip()

    def ingest(self, *extra, db=None) -> tuple[int, str]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = ingest.run(["--repo", str(self.repo), "--db", str(db or self.db), "--workers", "2", *extra])
        return code, out.getvalue()

    def ok(self, *extra, db=None) -> str:
        code, out = self.ingest(*extra, db=db)
        self.assertEqual(code, 0, out)
        return out

    def con(self, db=None) -> sqlite3.Connection:
        return sqlite3.connect(db or self.db)

    def q(self, sql, *args, db=None):
        with contextlib.closing(self.con(db)) as c:
            return c.execute(sql, args).fetchall()

    def dump(self, db=None, tables=None) -> dict:
        """Every row of every table, ids included (detects any rewrite)."""
        with contextlib.closing(self.con(db)) as c:
            names = tables or [r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'source_fts_%'")]
            return {t: sorted(map(repr, c.execute(f"SELECT * FROM {t}"))) for t in names}

    def canonical(self, db=None) -> dict:
        """Knowledge with row ids replaced by what they point at, to compare against a fresh build."""
        with contextlib.closing(self.con(db)) as c:
            ev = {r[0]: r[1:] for r in c.execute("SELECT id, path, line, quote, extractor, unit FROM evidence")}
            out = {
                "source": sorted(c.execute("SELECT path, kind, det_kind, archived, doc_date, git_date, bytes, text, "
                                           "doc_status, summary, blob FROM source")),
                "fts": sorted(c.execute("SELECT path, kind, text FROM source_fts")),
                "component": sorted(c.execute("SELECT * FROM component")),
                "evidence": sorted(map(repr, ev.values())),
                "relation": sorted(repr((r[:5], ev[r[5]], r[6:])) for r in c.execute(
                    "SELECT src, type, dst, status, confidence, evidence_id, props, unit FROM relation")),
                "claim": sorted(repr((r[:6], ev[r[6]], r[7])) for r in c.execute(
                    "SELECT component, topic, text, path, doc_date, doc_status, evidence_id, unit FROM claim")),
                "risk": sorted(repr((r[:6], ev[r[6]], r[7])) for r in c.execute(
                    "SELECT rule, severity, subject, description, guidance, confidence, evidence_id, unit FROM risk")),
                # verifier text may cite evidence ids (E12): compare without the numbers
                "conflict": sorted(repr((r[:3], re.sub(r"\bE\d+", "E#", r[3]), r[4:8],
                                         sorted(ev[e] for e in json.loads(r[8])))) for r in c.execute(
                    "SELECT kind, check_name, subject, description, confidence, verdict, rationale, unit, "
                    "evidence_ids FROM conflict")),
            }
        return out

    def assert_matches_fresh_build(self, *extra):
        """The incrementally maintained database equals a from-scratch build of the same commit."""
        fresh = self.tmp / f"fresh-{len(list(self.tmp.glob('fresh-*')))}.db"
        calls = Counter(self.fake.calls)
        self.ok(*extra, db=fresh)
        self.fake.calls = calls  # the reference build's model calls do not count
        a, b = self.canonical(), self.canonical(fresh)
        for table in a:
            self.assertEqual(a[table], b[table], f"{table} differs from a fresh build")

    def stage(self, name, db=None):
        return self.q("SELECT commit_id, status, detail FROM stage_state WHERE stage = ?", name, db=db)[0]

    def search(self, text) -> list[str]:
        return [r[0] for r in self.q("SELECT path FROM source_fts WHERE source_fts MATCH ?", text)]


# ----------------------------------------------------------------------------- tests

class IncrementalIngestTest(Base):
    def test_01_initial_ingestion_records_provenance(self):
        out = self.ok()
        head = self.git("rev-parse", "HEAD").strip()
        self.assertIn("initial build", out)
        self.assertEqual(self.stage("deterministic")[:2], (head, "complete"))
        self.assertEqual(self.stage("model")[:2], (head, "complete"))
        identity = self.git("rev-list", "--max-parents=0", "HEAD").strip()
        self.assertEqual(self.q("SELECT value FROM ingest_meta WHERE key = 'repo_identity'"), [(identity,)])
        for path, blob in self.q("SELECT path, blob FROM source"):
            self.assertEqual(blob, self.git("rev-parse", f"HEAD:{path}").strip(), path)
        self.assertEqual(self.q("PRAGMA user_version"), [(ingest.SCHEMA_VERSION,)])
        self.assertTrue(self.q("SELECT 1 FROM relation WHERE src = 'comp:order-service' AND type = 'CALLS' "
                               "AND dst = 'comp:user-service'"))
        self.assertEqual(self.search("zebracorn"), ["wiki/orders.md"])
        self.assertEqual(self.search("zebradiagram"), ["diagrams/architecture.png"])  # enrichment text indexed
        self.assertTrue(self.q("SELECT 1 FROM claim"))
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE verdict = 'llm-found'"))
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE check_name = 'code-call-missing-from-diagram'"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])

    def test_02_identical_run_does_nothing(self):
        self.ok()
        before, mtime = self.dump(), self.db.stat().st_mtime_ns
        calls = sum(self.fake.calls.values())
        out = self.ok()
        self.assertIn("nothing to do", out)
        self.assertEqual(sum(self.fake.calls.values()), calls, "no model calls")
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.db.stat().st_mtime_ns, mtime)
        # the model refined the document type; the rule-based one is kept apart, so nothing re-runs
        self.assertEqual(self.q("SELECT kind, det_kind FROM source WHERE path = 'wiki/orders.md'"), [("wiki", "doc")])
        self.assertEqual(self.q("SELECT kind FROM source_fts WHERE path = 'wiki/orders.md'"), [("wiki",)])

    def test_03_edit_updates_blob_and_affected_knowledge(self):
        self.ok()
        wiki_ev = self.q("SELECT id, quote FROM evidence WHERE path = 'wiki/orders.md' ORDER BY id")
        self.write({"apps/user-service/src/server.js":
                    CORPUS["apps/user-service/src/server.js"].replace("/v1/users/:id", "/v2/users/:id")})
        self.commit("user-service: v2 users route")
        self.fake.calls.clear()
        out = self.ok()
        self.assertIn("1 modified", out)
        self.assertIn("1 files re-extracted (1 content)", out)
        blob = self.git("rev-parse", "HEAD:apps/user-service/src/server.js").strip()
        self.assertEqual(self.q("SELECT blob FROM source WHERE path = 'apps/user-service/src/server.js'"), [(blob,)])
        self.assertEqual(self.q("SELECT blob FROM extraction_unit WHERE unit = 'det:apps/user-service/src/server.js'"),
                         [(blob,)])
        self.assertFalse(self.q("SELECT 1 FROM component WHERE id = 'endpoint:user-service GET /v1/users/{}'"))
        self.assertTrue(self.q("SELECT 1 FROM component WHERE id = 'endpoint:user-service GET /v2/users/{}'"))
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE check_name = 'call-to-missing-route'"))
        # unchanged files keep their rows (same ids), and no prose/diagram model calls were needed
        self.assertEqual(self.q("SELECT id, quote FROM evidence WHERE path = 'wiki/orders.md' ORDER BY id"), wiki_ev)
        self.assertEqual(self.fake.calls["prose"] + self.fake.calls["diagram"], 0)
        self.assertGreater(self.fake.calls["verify"], 0)
        self.assert_matches_fresh_build()

    def test_04_delete_removes_unsupported_knowledge(self):
        self.ok()
        self.assertTrue(self.q("SELECT 1 FROM component WHERE id = 'lib:cors'"))
        self.assertTrue(self.q("SELECT 1 FROM component WHERE id = 'concept:topic-orders'"))
        self.git("rm", "-q", "wiki/orders.md", "apps/user-service/package.json")
        self.commit("drop wiki page and user-service manifest")
        out = self.ok()
        self.assertIn("2 deleted", out)
        self.assertEqual(self.search("zebracorn"), [])
        self.assertFalse(self.q("SELECT 1 FROM evidence WHERE path IN ('wiki/orders.md', 'apps/user-service/package.json')"))
        self.assertFalse(self.q("SELECT 1 FROM relation WHERE dst = 'source:wiki/orders.md'"))
        self.assertFalse(self.q("SELECT 1 FROM claim WHERE path = 'wiki/orders.md'"))
        self.assertFalse(self.q("SELECT 1 FROM component WHERE id IN ('lib:cors', 'concept:topic-orders')"))
        self.assertFalse(self.q("SELECT 1 FROM extraction_unit WHERE path = 'wiki/orders.md'"))
        # still supported by other sources
        self.assertTrue(self.q("SELECT 1 FROM component WHERE id = 'lib:express'"))
        self.assertTrue(self.q("SELECT 1 FROM relation WHERE src = 'comp:order-service' AND type = 'MENTIONED_IN' "
                               "AND dst = 'source:docs/notes.md'"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assert_matches_fresh_build()

    def test_05_shared_config_change_refreshes_dependent_results(self):
        self.ok()
        literal = "SELECT 1 FROM relation r JOIN evidence e ON e.id = r.evidence_id WHERE r.type = 'CALLS' " \
                  "AND e.extractor = 'url-literal'"
        self.assertTrue(self.q(literal), "http://127.0.0.1:5002 resolves through the port registry")
        wiki_unit = self.q("SELECT blob, context FROM extraction_unit WHERE unit = 'det:wiki/orders.md'")
        self.write({"config/ports.json": json.dumps({"order-service": 5001, "user-service": 5003}, indent=2)})
        self.commit("move user-service to 5003")
        out = self.ok()
        self.assertRegex(out, r"re-extracted \(\d+ shared context, 1 content\)")
        self.assertFalse(self.q(literal), "nothing listens on :5002 any more")
        self.assertTrue(self.q("SELECT 1 FROM relation WHERE src = 'comp:order-service' AND type = 'CALLS' "
                               "AND dst = 'comp:user-service'"), "the env-var call still resolves by name")
        self.assertEqual(self.q("SELECT blob, context FROM extraction_unit WHERE unit = 'det:wiki/orders.md'"),
                         wiki_unit, "docs do not depend on ports and are reused")
        self.assert_matches_fresh_build()
        # deployment config: compose no longer declares the dependency the code has
        self.write({"compose.yaml": CORPUS["compose.yaml"].replace("    depends_on: [user-service]\n", "")
                    .replace("    image: user-service\n", "    image: user-service\n    depends_on: [order-service]\n")})
        self.commit("compose: invert dependency")
        self.ok()
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE check_name = 'call-not-in-deploy-deps' "
                               "AND subject = 'order-service -> user-service'"))
        self.assert_matches_fresh_build()

    def test_06_diagram_change_detected_by_blob(self):
        self.ok()
        self.write({"diagrams/architecture.png": PNG + b"v2"})
        self.commit("redraw architecture")
        self.fake.calls.clear()
        out = self.ok("--no-llm")
        self.assertIn("1 modified", out)
        self.assertIn("0 files re-extracted", out)
        self.assertIn("1 file results invalidated", out)
        blob = self.git("rev-parse", "HEAD:diagrams/architecture.png").strip()
        self.assertEqual(self.q("SELECT blob, text FROM source WHERE path = 'diagrams/architecture.png'"), [(blob, None)])
        self.assertEqual(self.search("zebradiagram"), [], "stale diagram description left the search index")
        self.assertFalse(self.q("SELECT 1 FROM relation WHERE type = 'DIAGRAM_EDGE'"))
        self.assertEqual(self.stage("model")[1], "incomplete")
        self.assertEqual(sum(self.fake.calls.values()), 0)
        self.ok()
        self.assertEqual(self.fake.calls["diagram"], 1)
        self.assertEqual(self.fake.calls["prose"], 0)
        self.assertEqual(self.search("zebradiagram"), ["diagrams/architecture.png"])
        self.assertEqual(self.stage("model")[1], "complete")
        self.assert_matches_fresh_build()

    def test_07_unprocessed_change_only_advances_snapshot(self):
        self.ok()
        knowledge = ["component", "relation", "evidence", "claim", "conflict", "risk", "component_decl", "unit_fact"]
        before = self.dump(tables=knowledge)
        self.write({"pnpm-lock.yaml": "lockfileVersion: '9.1'\n"})
        head = self.commit("bump lockfile")
        self.fake.calls.clear()
        out = self.ok()
        self.assertIn("0 files re-extracted", out)
        self.assertIn("checks not needed", out)
        self.assertEqual(sum(self.fake.calls.values()), 0)
        self.assertEqual(self.dump(tables=knowledge), before)
        self.assertEqual(self.stage("deterministic")[:2], (head, "complete"))
        self.assertEqual(self.stage("model")[:2], (head, "complete"))
        self.assertEqual(self.q("SELECT blob FROM source WHERE path = 'pnpm-lock.yaml'"),
                         [(self.git("rev-parse", "HEAD:pnpm-lock.yaml").strip(),)])

    def test_08_dirty_tracked_input_rejected_before_db_changes(self):
        self.ok("--no-llm")
        before, mtime = self.dump(), self.db.stat().st_mtime_ns
        (self.repo / "wiki/orders.md").write_text("edited but not committed\n")
        code, out = self.ingest()
        self.assertEqual(code, 1)
        self.assertIn("uncommitted changes to tracked files", out)
        self.git("add", "wiki/orders.md")  # staged is rejected as well
        self.assertEqual(self.ingest()[0], 1)
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.db.stat().st_mtime_ns, mtime)
        # a missing database is not even created
        code, _ = self.ingest(db=self.tmp / "new.db")
        self.assertEqual(code, 1)
        self.assertFalse((self.tmp / "new.db").exists())
        # untracked files are outside the corpus
        self.git("reset", "-q", "--hard")
        (self.repo / "scratch.md").write_text("untracked\n")
        self.assertIn("nothing to do", self.ok("--no-llm"))

    def test_09_legacy_schema_migrates(self):
        with contextlib.closing(self.con()) as c:
            for stmt in ingest._statements(LEGACY_V1_SCHEMA):  # the pre-provenance schema
                c.execute(stmt)
            c.execute(ingest.FTS_SCHEMA)
            c.execute("INSERT INTO source (path, kind, archived, bytes, text) VALUES ('gone.md', 'doc', 0, 5, 'hello')")
            c.execute("INSERT INTO source_fts VALUES ('gone.md', 'doc', 'hello')")
            c.execute("INSERT INTO evidence (path, line, quote, extractor) VALUES ('gone.md', 1, 'hello', 'x')")
            c.execute("INSERT INTO component VALUES ('app:gone', 'app', 'gone', NULL, NULL)")
            c.execute("INSERT INTO relation (src, type, dst, status, confidence, evidence_id) "
                      "VALUES ('app:gone', 'CALLS', 'app:gone', 'observed', 1, 1)")
            c.commit()
        out = self.ok("--no-llm")
        self.assertIn("legacy schema migrated", out)
        self.assertEqual(self.q("PRAGMA user_version"), [(ingest.SCHEMA_VERSION,)])
        self.assertFalse(self.q("SELECT 1 FROM source WHERE path = 'gone.md'"))
        self.assertFalse(self.q("SELECT 1 FROM component WHERE id = 'app:gone'"))
        self.assertEqual(self.search("hello"), [])
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assert_matches_fresh_build("--no-llm")
        self.assertIn("nothing to do", self.ok("--no-llm"))

    def test_10_failure_preserves_previous_state(self):
        self.ok()
        first = self.git("rev-parse", "HEAD").strip()
        before = self.dump()
        self.write({"apps/user-service/src/server.js": "// rewritten\n"})
        self.commit("rewrite")

        def boom(ctx):
            raise RuntimeError("simulated extractor crash")
        steps = [(boom if f is ingest.check_endpoints else f, scope) for f, scope in ingest.STEPS]
        with mock.patch.object(ingest, "STEPS", steps), self.assertRaises(RuntimeError):
            self.ingest()
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.stage("deterministic")[0], first)
        # a failed model stage publishes the deterministic update but not model completion
        self.fake.fail = True
        code, out = self.ingest()
        self.assertEqual(code, ingest.EXIT_INCOMPLETE, out)
        self.assertIn("simulated OpenRouter outage", out)
        head = self.git("rev-parse", "HEAD").strip()
        self.assertEqual(self.stage("deterministic")[:2], (head, "complete"))
        self.assertEqual(self.stage("model")[1], "incomplete")
        self.assertIn("failed", self.stage("model")[2])
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.fake.fail = False
        self.ok()
        self.assertEqual(self.stage("model")[1], "complete")

    def test_11_deterministic_only_then_enrichment(self):
        self.ok("--no-llm")
        self.assertEqual(sum(self.fake.calls.values()), 0)
        self.assertEqual(self.stage("model")[1], "incomplete")
        self.assertIn("not requested", self.stage("model")[2])
        self.assertIn("nothing to do", self.ok("--no-llm"))
        with mock.patch.object(llm, "available", lambda: False):
            code, out = self.ingest()
        self.assertEqual(code, ingest.EXIT_INCOMPLETE)
        self.assertIn("OPENROUTER_API_KEY is not set", out)
        self.assertEqual(self.stage("model")[1], "incomplete")
        self.assertEqual(sum(self.fake.calls.values()), 0)
        out = self.ok()  # same HEAD: only the missing enrichment runs
        self.assertIn("deterministic stage already complete", out)
        self.assertEqual(self.fake.calls["prose"], 2)  # wiki/orders.md, docs/notes.md
        self.assertEqual(self.fake.calls["diagram"], 1)
        self.assertGreater(self.fake.calls["verify"], 0)
        self.assertEqual(self.stage("model")[1], "complete")
        self.assertTrue(self.q("SELECT 1 FROM claim"))
        calls = sum(self.fake.calls.values())
        self.assertIn("nothing to do", self.ok())
        self.assertEqual(sum(self.fake.calls.values()), calls)
        self.assert_matches_fresh_build()

    def test_12_configuration_and_version_changes_invalidate(self):
        self.ok()
        rules = "SELECT * FROM {} WHERE unit LIKE 'det:%' AND unit != 'det:*'"  # file-local rule results
        before = {t: sorted(self.q(rules.format(t))) for t in ("relation", "evidence", "conflict", "risk")}
        # extraction version: every file is re-extracted; identical results keep their rows. The LLM
        # code units are re-run (they skip what the rules found in the same file); prose is not.
        self.fake.calls.clear()
        with mock.patch.object(ingest, "EXTRACTION_VERSION", "test-next"):
            out = self.ok()
        self.assertIn("extraction signature changed", out)
        self.assertRegex(out, r"(\d+) files re-extracted \(\1 version\)")
        self.assertEqual({t: sorted(self.q(rules.format(t))) for t in before}, before)
        self.assertEqual((self.fake.calls["code"], self.fake.calls["prose"], self.fake.calls["diagram"]), (2, 0, 0))
        with contextlib.ExitStack() as patches:
            patches.enter_context(mock.patch.object(ingest, "EXTRACTION_VERSION", "test-next"))
            # extraction model: code pass and prose re-extracted, diagrams untouched
            self.fake.calls.clear()
            patches.enter_context(mock.patch.object(llm, "EXTRACT_MODEL", "test/extract-model-2"))
            self.ok()
            self.assertEqual((self.fake.calls["code"], self.fake.calls["prose"], self.fake.calls["diagram"]), (2, 2, 0))
            self.assertTrue(all("test/extract-model-2" in r[0] for r in self.q(
                "SELECT signature FROM extraction_unit WHERE unit IN ('llm:wiki/orders.md', 'llm:docs/notes.md')")))
            # reasoning model: diagrams and verification re-run, code pass and prose reused
            self.fake.calls.clear()
            patches.enter_context(mock.patch.object(llm, "REASON_MODEL", "test/reason-model-2"))
            self.ok()
            self.assertEqual((self.fake.calls["code"], self.fake.calls["prose"], self.fake.calls["diagram"]), (0, 0, 1))
            self.assertGreater(self.fake.calls["verify"], 0)
            # prompt change: the signature follows the prompt text
            self.fake.calls.clear()
            patches.enter_context(mock.patch.object(enrich, "EXTRACT_SYSTEM", enrich.EXTRACT_SYSTEM + " "))
            self.ok()
            self.assertEqual((self.fake.calls["code"], self.fake.calls["prose"], self.fake.calls["diagram"]), (0, 2, 0))
            self.assertIn("nothing to do", self.ok())
        self.assertNotIn("sk-", json.dumps(self.q("SELECT * FROM stage_state")), "no credentials in signatures")

    def test_14_removed_component_not_kept_alive_by_model_output(self):
        self.ok()
        self.assertTrue(self.q("SELECT 1 FROM relation WHERE type = 'DIAGRAM_EDGE'"))  # diagram maps "Users"
        # user-service disappears; the diagram that draws it is unchanged
        self.git("rm", "-q", "-r", "apps/user-service")
        self.commit("remove user-service")
        self.ok()
        self.assertFalse(self.q("SELECT 1 FROM component WHERE id = 'comp:user-service'"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assert_matches_fresh_build()

    def test_13_queries_still_work(self):
        self.ok()
        db = sqlite3.connect(self.db)
        db.row_factory = sqlite3.Row

        def run(fn, arg):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                fn(db, arg)
            return buf.getvalue()

        self.assertIn("wiki/orders.md", run(query.search, "zebracorn"))
        self.assertIn("diagrams/architecture.png", run(query.search, "zebradiagram"))
        comp = run(query.component, "order-service")
        self.assertIn("CALLS -> comp:user-service", comp)
        self.assertIn("apps/order-service/src/server.js", comp)
        impact = run(query.impact, "user-service")
        self.assertIn("comp:order-service", impact)
        self.assertIn("Owner: NONE (gap)", impact)
        self.assertIn("total", run(query.conflicts, "total"))
        self.assertIn("cors-any-origin", run(query.risks, "user-service"))
        with mock.patch.object(query, "DB", self.db):
            ro = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
            self.assertIn("orders.md", query.run_tool(query.connect(), ro, "search", {"text": "zebracorn"}))
            self.assertIn("blob", query.run_tool(query.connect(), ro, "sql", {"query": "SELECT path, blob FROM source LIMIT 1"}))
            ro.close()
        db.close()


class HistoryAndCodePassTest(Base):
    def runs(self):
        return self.q("SELECT id, llm, new, resolved, baseline_run FROM run ORDER BY id")

    def test_15_run_history_is_recorded_in_place(self):
        self.ok()
        self.assertEqual(self.runs(), [(1, 1, 0, 0, None)])
        self.ok()  # no-op: no run row
        self.assertEqual(len(self.runs()), 1)
        self.write({"packages/shared/index.js": CORPUS["packages/shared/index.js"] + SECRET})
        self.commit("add a key")
        out = self.ok()
        self.assertIn("NEW       0 conflicts, 1 risks", out)
        self.assertEqual(self.runs()[-1], (2, 1, 1, 0, 1))
        change = self.q("SELECT item, change, summary, ref_id FROM change WHERE run_id = 2")
        self.assertEqual([(c[0], c[1]) for c in change], [("risk", "new")])
        self.assertIn("hardcoded-secret", change[0][2])
        self.assertTrue(self.q("SELECT 1 FROM risk WHERE id = ? AND rule = 'hardcoded-secret'", change[0][3]))
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), mock.patch.object(query, "DB", self.db):
            query.changes(query.connect())
        self.assertIn("+ NEW risk", buf.getvalue())
        self.write({"packages/shared/index.js": CORPUS["packages/shared/index.js"]})
        self.commit("remove the key")
        self.ok()
        self.assertEqual(self.runs()[-1], (3, 1, 0, 1, 2))
        self.assertEqual(self.q("SELECT item, change, ref_id FROM change WHERE run_id = 3"), [("risk", "resolved", None)])
        # rules-only run after a change: model stage incomplete -> not comparable, no false "resolved"
        self.write({"docs/notes.md": CORPUS["docs/notes.md"] + "More notes.\n"})
        self.commit("notes")
        self.ok("--no-llm")
        self.assertEqual(self.runs()[-1], (4, 0, 0, 0, None))
        self.ok()  # one run row per invocation, updated by the model transaction
        self.assertEqual(self.runs()[-1][:2], (5, 1))

    def test_16_teammate_schema_migrates_and_keeps_history(self):
        with contextlib.closing(self.con()) as c:  # the rebuild-per-run schema with run history
            for stmt in ingest._statements(ingest.SCHEMA + ingest.RUN_SCHEMA):
                c.execute(stmt)
            c.execute(ingest.FTS_SCHEMA)
            c.execute("INSERT INTO run VALUES (1, ?, 'abc1234', '2026-11-06T10:00:00', 0, 0, 0, 0, 0, NULL)",
                      (str(self.repo.resolve()),))
            c.execute("INSERT INTO change VALUES (1, 'risk', 'new', 'f00', 'old finding', NULL)")
            c.commit()
        out = self.ok("--no-llm")
        self.assertIn("legacy schema migrated", out)
        active = self.q("SELECT (SELECT COUNT(*) FROM conflict) + (SELECT COUNT(*) FROM risk)")[0][0]
        # compared with the teammate-era run (same repo, model stage incomplete in both): all findings are new
        self.assertEqual(self.runs(), [(1, 0, 0, 0, None), (2, 0, active, 0, 1)])
        self.assertEqual(self.q("SELECT summary FROM change WHERE run_id = 1"), [("old finding",)])
        self.assertTrue(self.q("SELECT 1 FROM risk WHERE fingerprint IS NOT NULL"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assert_matches_fresh_build("--no-llm")

    def test_17_code_pass_feeds_cross_source_checks(self):
        gap = "SELECT 1 FROM conflict WHERE check_name = 'route-not-in-spec' AND subject LIKE 'order-service DELETE%'"
        self.ok("--no-llm")
        self.assertFalse(self.q(gap))
        self.ok()  # the LLM code pass finds DELETE /v1/orders/{id}; the checks are recomputed
        self.assertTrue(self.q(gap))
        self.assertTrue(self.q("SELECT 1 FROM risk WHERE rule = 'code-review-llm'"))
        self.assertEqual(self.q("SELECT COUNT(*) FROM relation WHERE src = 'comp:order-service' AND type = 'CALLS' "
                                "AND props LIKE '%/v1/users/{}%'"), [(1,)], "the rules already found that call")
        self.assert_matches_fresh_build()
        # the file changes, rules only: the stale code-pass result and the gap it caused are gone
        self.write({"apps/order-service/src/server.js": "// v2\n" + CORPUS["apps/order-service/src/server.js"]})
        self.commit("touch order-service")
        self.ok("--no-llm")
        self.assertFalse(self.q(gap))
        self.assertFalse(self.q("SELECT 1 FROM risk WHERE rule = 'code-review-llm' AND subject = 'order-service'"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.ok()
        self.assertTrue(self.q(gap))
        self.assert_matches_fresh_build()


if __name__ == "__main__":
    unittest.main()
