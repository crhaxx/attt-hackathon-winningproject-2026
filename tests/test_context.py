"""context() retrieval, document aliases and claim perspective (temporary Git repos, mocked model).

    python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import contextlib
import io
import json
import re
import sqlite3
import unittest
from unittest import mock

from test_incremental import Base, FakeModel

import ingest  # noqa: E402  (path set up by test_incremental)
import llm  # noqa: E402
import query  # noqa: E402

ACCESS = """# Access

All storefront traffic enters Gate, the order-service front door.
The user-service will call Gate for order lookups after the migration.
Moving payments behind Gate has not reached a launch date.
"""
OLD = """# Old order flow
Status: superseded

The order-service writes orders straight to the payments ledger.
"""


class ContextModel(FakeModel):
    """FakeModel plus aliases ("X, the <component> ...") and perspectives, including invalid ones."""

    @staticmethod
    def prose(user):
        out = FakeModel.prose(user)
        doc = user.split("<document>\n", 1)[1].rsplit("\n</document>", 1)[0]
        lines = [l.strip() for l in doc.splitlines() if l.strip()]
        for line in lines:
            m = re.search(r"(\w+), the ([\w-]+)", line)
            if m:
                out["aliases"] = [{"name": m.group(1), "component": m.group(2), "quote": line},
                                  {"name": "Ghost", "component": m.group(2), "quote": line},  # not in the quote
                                  {"name": "user-service", "component": "order-service", "quote": line}]  # clash
        if "will call Gate" in doc:
            plan = next(l for l in lines if "will call Gate" in l)
            idea = next(l for l in lines if "launch date" in l)
            out["facts"].append({"subject": "user-service", "relation": "CALLS", "object": "Gate",
                                 "perspective": "intended", "quote": plan, "confidence": 0.6})
            out["claims"] += [{"component": "Gate", "topic": "ops", "claim": "payments behind Gate",
                               "perspective": "proposed", "quote": idea}]
        if "Status: superseded" in doc:
            out["doc_status"] = "superseded"
            line = next(l for l in lines if "ledger" in l)
            out["claims"] = [{"component": "order-service", "topic": "money", "claim": "writes the ledger",
                              "perspective": "current", "quote": line}]
        return out


class ContextTest(Base):
    def setUp(self):
        super().setUp()
        self.fake = ContextModel()
        p = mock.patch.object(llm, "chat", self.fake)
        p.start()
        self.addCleanup(p.stop)
        self.qdb = mock.patch.object(query, "DB", self.db)
        self.qdb.start()
        self.addCleanup(self.qdb.stop)

    def context(self, name, **kw):
        db = query.connect()
        try:
            if kw.pop("raw", False):
                return query.component_context(db, name, **kw)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                query.context(db, name, **kw)
            return buf.getvalue()
        finally:
            db.close()

    def test_context_package(self):
        self.ok()
        ctx = self.context("order-service", raw=True)
        self.assertEqual(ctx["component"]["id"], "comp:order-service")
        self.assertEqual(ctx["snapshot"]["deterministic"]["status"], "complete")
        types = {(r["type"], r["status"]) for r in ctx["relationships"]}
        self.assertIn(("CALLS", "observed"), types)
        self.assertTrue(ctx["claims"] and ctx["decisions"] == [])
        evidence = {e["id"] for e in ctx["evidence"]}
        for section in ("relationships", "claims", "findings"):
            for item in ctx[section]:
                self.assertTrue(set(item["evidence"]) <= evidence, f"{section} cites evidence not in the package")
        self.assertTrue(any("passage" in e for e in ctx["evidence"]))
        # findings are linked with a stated basis, by whole names only
        checks = {f.get("check") for f in ctx["findings"]}
        self.assertIn("doc-describes-dropped-column", checks)
        self.assertNotIn("no-owner", checks, "user-service's ownership gap is not order-service's")
        self.assertIn("no-owner", {f.get("check") for f in self.context("user-service", raw=True)["findings"]})
        self.assertTrue(all(f["linked_by"] for f in ctx["findings"]))
        text = self.context("order-service")
        self.assertIn("IMPLEMENTED (code/config):", text)
        self.assertRegex(text, r"E\d+ apps/order-service/src/server\.js:\d+")

    def test_budget_never_breaks_json(self):
        self.ok()
        db = query.connect()
        ro = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            for budget in (600, 2000, 50000):
                out = query.run_tool(db, ro, "context", {"name": "order-service", "format": "json", "max_chars": budget})
                data = json.loads(out)  # always whole
                if budget < 50000:
                    self.assertTrue(data["truncation"]["truncated"])
            small = json.loads(query.run_tool(db, ro, "context", {"name": "order-service", "format": "json",
                                                                  "max_chars": 2000}))
            self.assertEqual(small["truncation"]["passages"], "omitted to fit the size budget")
            self.assertIn("unknown component", query.run_tool(db, ro, "context", {"name": "nope"}))
        finally:
            db.close()
            ro.close()

    def test_focus_reorders_but_hides_nothing(self):
        self.ok()
        plain = self.context("order-service", raw=True, max_items=50)
        focused = self.context("order-service", raw=True, max_items=50, focus="users")
        for section in ("relationships", "claims", "findings"):
            self.assertEqual(sorted(i["id"] for i in plain[section]), sorted(i["id"] for i in focused[section]))
        self.assertIn("users", json.dumps(focused["relationships"][0]))

    def test_aliases_and_perspective(self):
        self.write({"docs/access.md": ACCESS})
        self.commit("access doc")
        self.ok()
        aliases = self.q("SELECT src, json_extract(props, '$.alias') FROM relation WHERE type = 'KNOWN_AS'")
        self.assertEqual(aliases, [("comp:order-service", "Gate")], "unquoted and clashing aliases are rejected")
        ctx = self.context("Gate", raw=True)
        self.assertEqual(ctx["component"]["id"], "comp:order-service")
        self.assertIn("called 'Gate' in docs/access.md", ctx["component"]["matched_by"])
        db = query.connect()
        self.assertEqual(query.resolve(db, "Gate"), "comp:order-service")
        db.close()
        # the document's own facts/claims resolve "Gate" through its alias; perspectives are kept per sentence
        persp = dict(self.q("SELECT text, perspective FROM claim WHERE path = 'docs/access.md'"))
        self.assertEqual(persp.get("payments behind Gate"), "proposed")
        self.assertEqual(self.q("SELECT component FROM claim WHERE text = 'payments behind Gate'"),
                         [("comp:order-service",)])
        planned = self.q("SELECT status, json_extract(props, '$.perspective') FROM relation WHERE "
                         "src = 'comp:user-service' AND type = 'CALLS' AND dst = 'comp:order-service'")
        self.assertEqual(planned, [("declared", "intended")])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            query.impact(query.connect(), "order-service")
        self.assertIn("Affected: 0 direct dependents", out.getvalue(), "a plan is not a dependency")
        self.assertIn("<proposed>", self.context("Gate"))
        self.assert_matches_fresh_build()
        # the alias lives and dies with its document
        self.git("rm", "-q", "docs/access.md")
        self.commit("drop access doc")
        self.ok()
        self.assertFalse(self.q("SELECT 1 FROM relation WHERE type = 'KNOWN_AS'"))
        self.assertIn("unknown component: Gate", self.context("Gate"))
        self.assert_matches_fresh_build()

    def test_ambiguous_alias_returns_candidates(self):
        self.write({"docs/access.md": ACCESS,
                    "docs/profiles.md": "# Profiles\n\nProfile reads go to Gate, the user-service read path.\n"})
        self.commit("two docs, one name")
        self.ok()
        ctx = self.context("Gate", raw=True)
        self.assertEqual(ctx.get("ambiguous"), "Gate")
        self.assertEqual({c["id"] for c in ctx["candidates"]}, {"comp:order-service", "comp:user-service"})
        self.assertIn("is ambiguous", self.context("Gate"))

    def test_historical_document_claims(self):
        self.write({"docs/old.md": OLD})
        self.commit("old doc")
        self.ok()
        self.assertEqual(self.q("SELECT perspective FROM claim WHERE path = 'docs/old.md'"), [("historical",)],
                         "'current' inside a superseded document describes the past")

    def test_schema_v2_upgrades_in_place(self):
        self.ok("--no-llm")
        with contextlib.closing(self.con()) as c:
            c.execute("ALTER TABLE claim DROP COLUMN perspective")
            c.execute("PRAGMA user_version = 2")
            c.commit()
        out = self.ok("--no-llm")
        self.assertIn("schema upgraded v2 -> v3", out)
        self.assertIn("0 files re-extracted", out)
        self.assertEqual(self.q("PRAGMA user_version"), [(ingest.SCHEMA_VERSION,)])
        self.assertIn("perspective", {r[1] for r in self.q("PRAGMA table_info(claim)")})
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assertIn("nothing to do", self.ok("--no-llm"))

    def test_names_component(self):
        yes = [("order-service", "order-service"), ("order-service.orders.total", "order-service"),
               ("api-gateway -> order-service", "order-service"), ("comp:ui", "ui"),
               ("order-service.orders.total", "order-service.orders")]
        no = [("order-service-legacy.x", "order-service"), ("build pipeline", "ui"),
              ("GET /v1/orders", "orders"), ("user-service", "service")]
        for subject, short in yes:
            self.assertTrue(query.names_component(subject, short), (subject, short))
        for subject, short in no:
            self.assertFalse(query.names_component(subject, short), (subject, short))


if __name__ == "__main__":
    unittest.main()
