"""Policy checks (policy.py): six outcomes, scope and authority rules, incremental behaviour. Mocked model.

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
import policy  # noqa: E402
import query  # noqa: E402

CALL = "comp:order-service -> comp:user-service GET /v1/users/{}"  # observed in the fixture's server.js


class PolicyModel(FakeModel):
    """FakeModel plus decisions from lines like
    'Rule: template=require_mediation actor=order-service target=user-service via=shared ops=reads status=accepted
     title=Users via shared'  (ops comma-separated; 'ops=' means the document does not say)."""

    @staticmethod
    def prose(user):
        out = FakeModel.prose(user)
        doc = user.split("<document>\n", 1)[1].rsplit("\n</document>", 1)[0]
        out["decisions"] = []
        for line in doc.splitlines():
            if not line.startswith("Rule:"):
                continue
            kv = dict(re.findall(r"(\w+)=((?:(?!\s\w+=).)*)", line[5:]))
            check = {"template": kv["template"], "actor": [kv["actor"]], "target": kv["target"],
                     "operations": [o for o in kv.get("ops", "").split(",") if o]}
            if kv.get("via"):
                check["via"] = kv["via"]
            out["decisions"].append({"title": kv["title"].strip(), "kind": "policy", "status": kv["status"],
                                     "rule": kv["title"].strip(), "applies_to": [kv["target"]],
                                     "supersedes": None, "check": check, "quote": line.strip()})
        return out


def rule(template, ops, status="accepted", title=None, actor="order-service", via="shared"):
    title = title or f"{template} {ops or 'unstated'} {status}"
    v = f" via={via}" if template == "require_mediation" else ""
    return f"Rule: template={template} actor={actor} target=user-service{v} ops={ops} status={status} title={title}\n"


class PolicyTest(Base):
    def setUp(self):
        super().setUp()
        self.fake = PolicyModel()
        p = mock.patch.object(llm, "chat", self.fake)
        p.start()
        self.addCleanup(p.stop)

    def policies(self, *rules: str, path="docs/policy.md"):
        self.write({path: "# Policy\n\n" + "".join(rules)})
        self.commit(f"policy {len(rules)}")
        self.ok()

    def outcome(self, subject=CALL):
        rows = self.q("SELECT outcome, reason FROM assessment WHERE subject = ?", subject)
        self.assertEqual(len(rows), 1, rows)
        return rows[0]

    def test_fixture_call_is_observed(self):
        self.ok()
        self.assertIn(("GET", "/v1/users/{}"), self.q(
            "SELECT json_extract(props, '$.method'), json_extract(props, '$.path') FROM relation "
            "WHERE src = 'comp:order-service' AND dst = 'comp:user-service' AND type = 'CALLS' "
            "AND json_extract(props, '$.path') IS NOT NULL"))
        self.assertFalse(self.q("SELECT 1 FROM assessment"), "no rules, no assessments")

    def test_violated_becomes_a_finding(self):
        self.policies(rule("require_mediation", "reads", title="Users only via shared"))
        outcome, reason = self.outcome()
        self.assertEqual(outcome, "violated")
        self.assertIn("Users only via shared", reason)
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE check_name = 'policy-violated' AND subject = ?", CALL))
        evs = json.loads(self.q("SELECT evidence_ids FROM assessment WHERE subject = ?", CALL)[0][0])
        paths = {r[0] for r in self.q(f"SELECT path FROM evidence WHERE id IN ({','.join(map(str, evs))})")}
        self.assertEqual(paths, {"apps/order-service/src/server.js", "docs/policy.md"}, "call and rule are both cited")

    def test_scoped_exception(self):
        self.policies(rule("require_mediation", "reads"), rule("permit_direct_call", "reads:users", title="Profile reads"))
        outcome, reason = self.outcome()
        self.assertEqual(outcome, "permitted_by_exception")
        self.assertIn("Profile reads", reason)
        self.assertFalse(self.q("SELECT 1 FROM conflict WHERE check_name LIKE 'policy-%'"))

    def test_exception_does_not_spread(self):
        # a write exception does not cover the read; an exception for another caller does not either
        self.policies(rule("require_mediation", "reads"), rule("permit_direct_call", "writes:adjust"),
                      rule("permit_direct_call", "all", actor="shared", title="Shared may call users"))
        self.assertEqual(self.outcome()[0], "violated")

    def test_authority(self):
        self.policies(rule("require_mediation", "reads", status="proposed"))
        outcome, reason = self.outcome()
        self.assertEqual(outcome, "not_applicable")
        self.assertIn("not in force", reason)
        self.assertFalse(self.q("SELECT 1 FROM conflict WHERE check_name LIKE 'policy-%'"))
        self.policies(rule("require_mediation", "reads", status="unknown"))
        self.assertEqual(self.outcome()[0], "unknown", "unknown authority gives no verdict")

    def test_unstated_scope_is_never_a_wildcard(self):
        self.policies(rule("require_mediation", ""))
        self.assertEqual(self.outcome()[0], "unknown")
        self.assertEqual(self.q("SELECT outcome FROM assessment WHERE scope = 'rule'"), [("unknown",)])

    def test_out_of_scope_and_satisfied(self):
        self.policies(rule("require_mediation", "writes", title="Writes via shared"))
        self.assertEqual(self.outcome()[0], "not_applicable", "a GET is outside a writes-only rule")
        rows = self.q("SELECT outcome, reason FROM assessment WHERE scope = 'rule'")
        self.assertEqual(rows[0][0], "satisfied")
        self.assertIn("none of the", rows[0][1])

    def test_conflicting_rules(self):
        self.policies(rule("require_mediation", "reads", via="shared", title="Via shared"),
                      rule("require_mediation", "reads", via="order-service", title="Via orders"))
        self.assertEqual(self.outcome()[0], "conflicting_rules")
        self.assertTrue(self.q("SELECT 1 FROM conflict WHERE check_name = 'policy-conflict'"))

    def test_unknown_party_is_not_guessed(self):
        self.policies(rule("require_mediation", "reads", actor="operations console"))
        self.assertFalse(self.q("SELECT 1 FROM assessment WHERE scope = 'call'"))
        reason = self.q("SELECT reason FROM assessment WHERE scope = 'rule'")[0][0]
        self.assertIn("unresolved: actor 'operations console'", reason)

    def test_follows_documents_incrementally(self):
        self.policies(rule("require_mediation", "reads"))
        self.assertEqual(self.outcome()[0], "violated")
        self.write({"docs/exception.md": "# Exception\n\n" + rule("permit_direct_call", "reads", title="Exception")})
        self.commit("add exception")
        self.ok()
        self.assertEqual(self.outcome()[0], "permitted_by_exception", "a new exception is picked up")
        self.assert_matches_fresh_build()
        self.git("rm", "-q", "docs/exception.md")
        self.commit("drop exception")
        self.ok()
        self.assertEqual(self.outcome()[0], "violated", "and dropped with its document")
        self.assert_matches_fresh_build()
        self.git("rm", "-q", "docs/policy.md")
        self.commit("drop policy")
        self.ok()
        self.assertFalse(self.q("SELECT 1 FROM assessment"))
        self.assertEqual(ingest.integrity_problems(self.con()), [])
        self.assertIn("nothing to do", self.ok())

    def test_context_shows_assessments(self):
        self.policies(rule("require_mediation", "reads", title="Users only via shared"))
        with mock.patch.object(query, "DB", self.db):
            db = query.connect()
            ctx = query.component_context(db, "order-service")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                query.context(db, "user-service")
            db.close()
        calls = self.q("SELECT COUNT(*) FROM assessment WHERE scope = 'call'")[0][0]  # one per observed GET call
        self.assertEqual([a["outcome"] for a in ctx["policy_assessments"]], ["violated"] * calls)
        evidence = {e["id"] for e in ctx["evidence"]}
        self.assertTrue(set(ctx["policy_assessments"][0]["evidence"]) <= evidence)
        self.assertIn("[VIOLATED] " + CALL, buf.getvalue())

    def test_one_observation_per_call_site(self):
        # live corpus: the rule-based extractor (method unknown) and the AI code pass (GET) both report line 39
        db = sqlite3.connect(":memory:")
        db.executescript("""
            CREATE TABLE component (id TEXT PRIMARY KEY, kind TEXT, name TEXT);
            CREATE TABLE evidence (id INTEGER PRIMARY KEY, path TEXT, line INTEGER);
            CREATE TABLE relation (id INTEGER PRIMARY KEY, src TEXT, type TEXT, dst TEXT, status TEXT,
                                   props TEXT, evidence_id INTEGER);
            INSERT INTO component VALUES ('policy:writes-via-gw', 'policy', 'Writes via gateway');
            INSERT INTO evidence VALUES (1, 'apps/admin/src/app/index.tsx', 39), (2, 'apps/admin/src/app/index.tsx', 39),
                                        (3, 'docs/adr.md', 3);
            INSERT INTO relation VALUES
              (1, 'comp:admin', 'CALLS', 'comp:inv', 'observed', '{"method": "*", "path": "/v1/products"}', 1),
              (2, 'comp:admin', 'CALLS', 'comp:inv', 'observed', '{"method": "GET", "path": "/v1/products"}', 2),
              (3, 'policy:writes-via-gw', 'STATED_IN', 'source:docs/adr.md', 'declared',
               '{"decision_status": "accepted", "check": {"template": "require_mediation", "actor": ["comp:admin"],
                 "target": "comp:inv", "via": "comp:gw", "operations": ["reads"], "unresolved": []}}', 3);
        """)
        written = []
        store = mock.Mock(db=db)
        store.assessment.side_effect = lambda *a: written.append(a)
        policy.evaluate(store)
        calls = [a for a in written if a[0] == "call"]
        self.assertEqual(len(calls), 1, written)
        scope, actor, target, subject, outcome, reason, rules, evs = calls[0]
        self.assertEqual(subject, "comp:admin -> comp:inv GET /v1/products", "the specific reading is kept")
        self.assertEqual(outcome, "violated")
        self.assertEqual(sorted(evs), [1, 2, 3], "both readings and the rule are cited")
        self.assertEqual(store.conflict.call_count, 1)

    def test_evaluator_change_is_picked_up_without_model_calls(self):
        self.policies(rule("require_mediation", "reads"))
        calls = sum(self.fake.calls.values())
        with mock.patch.object(ingest, "POLICY_SIGNATURE", "policy evaluator vtest"):
            out = self.ok()
            self.assertNotIn("nothing to do", out, "a new evaluator version invalidates stored assessments")
            self.assertEqual(self.q("SELECT signature FROM extraction_unit WHERE unit = ?", ingest.POLICY_CHECK),
                             [("policy evaluator vtest",)])
            self.assertEqual(sum(self.fake.calls.values()), calls, "re-evaluating needs no model calls")
            self.assertEqual(self.outcome()[0], "violated")
            self.assertIn("nothing to do", self.ok())

    def test_progress_counts_decision_statements(self):
        self.write({"docs/policy.md": "# Policy\n\n" + rule("require_mediation", "reads")})
        self.commit("policy")
        code, out = self.ingest()
        self.assertEqual(code, 0, out)
        self.assertIn("incl. 1 decision/policy statements", out)

    def test_operation_matching(self):
        P = policy.parse_operation
        self.assertIsNone(P("stock adjustments"))
        self.assertIsNone(P(""))
        self.assertTrue(policy.op_applies(P("writes:adjust"), "POST", "/v1/products/{}/adjustments"))
        self.assertFalse(policy.op_applies(P("writes:adjust"), "GET", "/v1/products"))
        self.assertFalse(policy.op_applies(P("writes"), "GET", "/v1/products"))
        self.assertIsNone(policy.op_applies(P("reads"), "*", "/v1/products"), "unknown method: cannot tell")
        self.assertTrue(policy.op_applies(P("GET /v1/products/:sku"), "GET", "/v1/products/{}"))
        self.assertFalse(policy.op_applies(P("GET /v1/products"), "GET", "/v1/orders"))
        r = policy.Rule("p", "t", "d.md", "accepted", 1, "require_mediation", ["a"], "b", "c", [])
        self.assertIsNone(r.applies("GET", "/x"), "empty scope is unknown, not all")


if __name__ == "__main__":
    unittest.main()
