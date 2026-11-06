#!/usr/bin/env python3
"""Result-quality gate for context.db (read-only).

Must-detect (D*) and must-not-detect (N*) checks for the intentionally imperfect hackathon repo, plus
quality metrics. Everything is keyed on paths, subjects and component ids - never on row ids or LLM
fingerprints, which change between runs.

    python3 tests/quality_check.py --summary                   human-readable
    python3 tests/quality_check.py [DB] > before.json           baseline
    python3 tests/quality_check.py [DB] > after.json            same source commit, after a change
    python3 tests/quality_check.py --compare before.json after.json   exit 1 on any regression

A regression is a check going PASS -> FAIL, or a metric moving the wrong way (with a tolerance for
metrics that vary between model runs). Checks that FAIL today are improvement targets.
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = str(Path(__file__).resolve().parent.parent / "context.db")
DEP = ("DEPENDS_ON", "CALLS", "CONFIGURED_TO_CALL", "DEPLOY_DEPENDS_ON")      # edges query.impact() follows
LINK = DEP + ("SHARES_DATA_WITH", "READS", "WRITES")
NO_OWNER = ("admin", "api-gateway", "customer-mobile", "employee-mobile", "storefront", "config-eslint",
            "config-typescript", "demo-identity", "http-client", "jest-presets", "logger", "ui")
CORS = ("api-gateway", "hr-service", "inventory-service", "order-service", "payment-service", "user-service")
OBSERVED_CALLS = {  # ground truth from code + compose.yaml at 612cc0b
    ("comp:admin", "comp:api-gateway"), ("comp:admin", "comp:inventory-service"),
    ("comp:api-gateway", "comp:inventory-service"), ("comp:api-gateway", "comp:order-service"),
    ("comp:api-gateway", "comp:payment-service"), ("comp:api-gateway", "comp:user-service"),
    ("comp:customer-mobile", "comp:api-gateway"), ("comp:employee-mobile", "comp:hr-service"),
    ("comp:hr-portal", "comp:hr-service"), ("comp:hr-service", "comp:user-service"),
    ("comp:order-service", "comp:inventory-service"), ("comp:order-service", "comp:payment-service"),
    ("comp:order-service", "comp:user-service"), ("comp:storefront", "comp:api-gateway")}
EXPECTED_DEPENDENTS = {  # first hop of impact(): who really calls / imports the component
    "comp:user-service": {"comp:order-service", "comp:api-gateway", "comp:hr-service"},
    "comp:payment-service": {"comp:order-service", "comp:api-gateway"},
    "comp:inventory-service": {"comp:order-service", "comp:api-gateway", "comp:admin"},
    "comp:order-service": {"comp:api-gateway"},
    "comp:api-gateway": {"comp:storefront", "comp:customer-mobile", "comp:admin"},
    "comp:hr-service": {"comp:hr-portal", "comp:employee-mobile"},
    "comp:demo-identity": {"comp:api-gateway", "comp:order-service", "comp:payment-service", "comp:user-service"},
    "comp:http-client": {"comp:api-gateway", "comp:order-service", "comp:payment-service", "comp:user-service"},
    "comp:logger": {"comp:api-gateway", "comp:order-service", "comp:payment-service", "comp:storefront",
                    "comp:user-service"},
    "comp:ui": {"comp:admin", "comp:blog", "comp:hr-portal", "comp:storefront"}}
CLIENT_BACKENDS = {"comp:storefront": {"comp:api-gateway"}, "comp:customer-mobile": {"comp:api-gateway"},
                   "comp:employee-mobile": {"comp:hr-service"}, "comp:hr-portal": {"comp:hr-service"},
                   "comp:admin": {"comp:api-gateway", "comp:inventory-service"}}
OFFTOPIC_KW = re.compile(r"auth|identit|header|ownership check|IDOR|PII|e-?mail|\blog|idempot|duplicate charge|double|"
                         r"minor.unit|\btotal\b|stale|cache|credential|secret|SSO|spoof|inject|expos|leak|bypass|"
                         r"retr(y|ies)|imperson|another customer|not own|access|write path|offboard", re.I)
# checks whose PASS rests on llm-found rows, vision output or verifier verdicts: re-run once before calling it a regression
LLM_DEPENDENT = {"D01", "D05", "D06", "D07-user-service", "D07-payment-service", "D07-inventory-service", "D09", "D10",
                 "D11", "D14b", "D15", "D15b", "D16", "D16b", "D16c", "D17", "D21", "D22", "D23", "D24", "D25",
                 "D26", "D27"}


def connect(path):
    db = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    db.row_factory = sqlite3.Row
    return db


def rows(db, sql, *args):
    return [dict(r) for r in db.execute(sql, args)]


def conflicts(db, path=None, text=None, kind=None, check=None, subject=None, verdict=None, active=True):
    sql = ("SELECT DISTINCT c.id, c.kind, c.check_name, c.subject, c.verdict, c.confidence, c.description "
           "FROM conflict c")
    args = []
    if path:
        sql += " JOIN json_each(c.evidence_ids) j JOIN evidence e ON e.id = j.value AND e.path = ?"
        args.append(path)
    sql += " WHERE 1 = 1"
    for col, val in (("c.kind", kind), ("c.check_name", check), ("c.subject", subject), ("c.verdict", verdict)):
        if val is not None:
            sql += " AND %s = ?" % col
            args.append(val)
    if active:                                   # NULL (unverified) counts as active, as in query.py
        sql += " AND c.verdict IS NOT ?"
        args.append("rejected")
    out = rows(db, sql, *args)
    if text:
        out = [r for r in out if re.search(text, r["subject"] + " " + r["description"], re.I)]
    return out


def risks(db, rule=None, subject=None, path=None, text=None, sev=None):
    sql = ("SELECT r.id, r.rule, r.severity, r.subject, r.description, r.confidence, e.path, e.line, e.quote "
           "FROM risk r JOIN evidence e ON e.id = r.evidence_id WHERE 1 = 1")
    args = []
    for col, val in (("r.rule", rule), ("r.subject", subject), ("e.path", path)):
        if val is not None:
            sql += " AND %s = ?" % col
            args.append(val)
    out = rows(db, sql, *args)
    if sev:
        out = [r for r in out if r["severity"] in sev]
    if text:
        out = [r for r in out if re.search(text, r["description"], re.I)]
    return out


def links(db, src, dst, types=LINK, statuses=("observed", "declared", "outdated")):
    tm, sm = ",".join("?" * len(types)), ",".join("?" * len(statuses))
    return rows(db, "SELECT r.id, r.src, r.type, r.dst, r.status, r.confidence, e.path, e.line FROM relation r "
                    "JOIN evidence e ON e.id = r.evidence_id WHERE r.src = ? AND r.dst = ? "
                    "AND r.type IN (%s) AND r.status IN (%s)" % (tm, sm), src, dst, *types, *statuses)


def direct_dependents(db, cid):
    """First hop of query.impact(): uses query.dependents() when it exists, else the plain traversal."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        import query
        if hasattr(query, "dependents"):
            return {src for src, _ in query.dependents(db, cid)}
    except Exception:
        pass
    tm = ",".join("?" * len(DEP))
    return {r["src"] for r in rows(db, "SELECT src FROM relation WHERE dst = ? AND type IN (%s) AND status != ? "
                                       "AND src != dst" % tm, cid, *DEP, "outdated")}


def has(rs):
    return bool(rs), sorted({r["id"] for r in rs})[:15]


CHECKS = []


def check(cid, group, desc):
    def deco(fn):
        CHECKS.append((cid, group, desc, fn))
        return fn
    return deco


# ------------------------------------------------------------ MUST DETECT
@check("D01", "detect", "orders.total stale in wiki/orders-client.md: rule finding confirmed")
def _(db):
    return has(conflicts(db, path="wiki/orders-client.md", check="doc-describes-dropped-column",
                         subject="order-service.orders.total", verdict="confirmed"))


@check("D02", "detect", "orders.total finding cites 002-minor-units.sql DROP COLUMN total; live column total_minor")
def _(db):
    ev = rows(db, "SELECT id FROM evidence WHERE path = ? AND quote LIKE ?",
              "apps/order-service/migrations/002-minor-units.sql", "%DROP COLUMN total%")
    cols = json.loads(rows(db, "SELECT props FROM component WHERE id = ?", "table:order-service.orders")[0]["props"])["columns"]
    rs = conflicts(db, path="apps/order-service/migrations/002-minor-units.sql", text=r"\btotal\b")
    return bool(ev) and "total_minor" in cols and "total" not in cols and bool(rs), [r["id"] for r in rs]


@check("D03", "detect", "wiki/accounts-http.md documents removed GET /v1/users (code serves GET /v2/users/:id)")
def _(db):
    v2 = rows(db, "SELECT 1 FROM relation WHERE src = ? AND type = ? AND dst = ? AND status = ?",
              "comp:user-service", "EXPOSES", "endpoint:user-service GET /v2/users/{}", "observed")
    ok, ids = has(conflicts(db, path="wiki/accounts-http.md", text=r"/v1/users"))
    return ok and bool(v2), ids


@check("D04", "detect", "wiki/accounts-http.md: status renamed to account_status")
def _(db):
    return has(conflicts(db, path="wiki/accounts-http.md", text=r"account_status|users\.status"))


@check("D05", "detect", "docs/reference/customer-schema.md stale (status / no tier)")
def _(db):
    return has(conflicts(db, path="docs/reference/customer-schema.md", text=r"status|tier"))


@check("D06", "detect", "wiki/inventory-service.md Redis cache removed by ADR-003")
def _(db):
    return has(conflicts(db, path="wiki/inventory-service.md", text=r"cache|redis"))


def _gateway_dep(t):
    @check("D07-" + t, "detect", "api-gateway calls %s but compose depends_on lists only order-service" % t)
    def _(db):
        return has([c for c in conflicts(db)
                    if re.search(r"api-gateway\s*(->|calls|is wired to)\s*(comp:)?%s\b" % t,
                                 c["subject"] + " " + c["description"])
                    and re.search(r"depends_on|deploy", c["description"], re.I)])


for _t in ("user-service", "payment-service", "inventory-service"):
    _gateway_dep(_t)


@check("D08", "detect", "no-owner gap for all 12 components without CODEOWNERS")
def _(db):
    missing = [s for s in NO_OWNER if not conflicts(db, check="no-owner", subject=s)]
    return not missing, missing


@check("D09", "detect", "IDOR: GET /v1/orders/:id lacks customer-ownership check (high risk, order-service index.ts)")
def _(db):
    return has(risks(db, subject="order-service", path="apps/order-service/src/index.ts", sev=("high",),
                     text=r"customer_id|ownership|IDOR|another customer"))


@check("D10", "detect", "IDOR also reported as code-vs-policy conflict citing order-service index.ts")
def _(db):
    return has(conflicts(db, path="apps/order-service/src/index.ts", text=r"ownership|customer_id"))


@check("D11", "detect", "identity taken from client header x-demo-user-id (demo-identity / gateway / order-service)")
def _(db):
    return has([r for p in ("packages/demo-identity/src/index.ts", "apps/api-gateway/src/index.ts",
                            "apps/order-service/src/index.ts")
                for r in risks(db, path=p, sev=("high", "medium"), text=r"header")
                if re.search(r"identity|impersonat|spoof", r["description"], re.I)])


@check("D11b", "detect", "deterministic rule client-asserted-identity fires (survives --no-llm)")
def _(db):
    return has(risks(db, rule="client-asserted-identity"))


@check("D12", "detect", "cors-any-origin rule on all 6 backends")
def _(db):
    missing = [s for s in CORS if not risks(db, rule="cors-any-origin", subject=s)]
    return not missing, missing


@check("D13", "detect", "hr-service logs whole employee record (Main.java directory.lookup)")
def _(db):
    return has([r for r in risks(db, rule="sensitive-data-in-logs", subject="hr-service",
                                 path="apps/hr-service/src/Main.java") if "directory.lookup" in r["quote"]])


@check("D14", "detect", "inventory-service OWNED_BY supply-operations observed from CODEOWNERS")
def _(db):
    return has(rows(db, "SELECT id FROM relation WHERE src = ? AND type = ? AND dst = ? AND status = ?",
                    "comp:inventory-service", "OWNED_BY", "team:@northstar/supply-operations", "observed"))


@check("D14b", "detect", "stale ownership in wiki/inventory-service.md (Core Platform) flagged")
def _(db):
    return has(conflicts(db, path="wiki/inventory-service.md", text=r"core platform|owner|escalat"))


@check("D15", "detect", "payment idempotency violated (conflict citing incident or PAY-118)")
def _(db):
    return has([c for p in ("incidents/2026-08-19-payment-retry.md", "issues/PAY-118.md")
                for c in conflicts(db, path=p, text=r"idempot")])


@check("D15b", "detect", "payment idempotency risk on apps/payment-service/src/index.ts")
def _(db):
    return has(risks(db, path="apps/payment-service/src/index.ts", text=r"idempot"))


@check("D16", "detect", "architecture-january.png outdated vs code (any active conflict)")
def _(db):
    return has(conflicts(db, path="diagrams/architecture-january.png"))


@check("D16b", "detect", "january diagram edge Storefront -> Orders flagged")
def _(db):
    return has(conflicts(db, path="diagrams/architecture-january.png", text=r"storefront[^.]*(order|checkout)"))


@check("D16c", "detect", "january diagram edge Storefront -> Accounts flagged")
def _(db):
    return has(conflicts(db, path="diagrams/architecture-january.png", text=r"storefront[^.]*(accounts|user-service)"))


@check("D17", "detect", "architecture-september.png omits admin -> inventory-service (ADR-005 exception)")
def _(db):
    return has(conflicts(db, path="diagrams/architecture-september.png", text=r"admin[^.]*inventory"))


@check("D18", "detect", ".agents/ prompt-injection risk")
def _(db):
    return has(risks(db, rule="agent-instructions-in-corpus", subject=".agents"))


@check("D19", "detect", "migration replay: users has account_status + customer_tier, no status")
def _(db):
    cols = json.loads(rows(db, "SELECT props FROM component WHERE id = ?", "table:user-service.users")[0]["props"])["columns"]
    return "account_status" in cols and "customer_tier" in cols and "status" not in cols, cols


@check("D20", "detect", "observed call graph equals the 14 real service pairs")
def _(db):
    got = {(r["src"], r["dst"]) for r in rows(db, "SELECT DISTINCT src, dst FROM relation WHERE status = ? "
                                                  "AND type IN (?, ?, ?)", "observed", *DEP[1:])}
    return got == OBSERVED_CALLS, {"missing": sorted(OBSERVED_CALLS - got), "extra": sorted(got - OBSERVED_CALLS)}


@check("D21", "detect", "exact-stock rule (spec) contradicted by INV-207 / final-unit incident")
def _(db):
    return has(conflicts(db, text=r"INV-207|exactly equal|final[- ]unit|last unit"))


@check("D21b", "detect", "root cause cited: app.py availability check uses <= (rejects the last unit)")
def _(db):
    ev = {r["id"] for r in rows(db, "SELECT id FROM evidence WHERE path = ? AND quote LIKE ?",
                                "apps/inventory-service/app.py", "%<= item.quantity%")}
    used = {r["evidence_id"] for r in rows(db, "SELECT evidence_id FROM risk")} | {
        j for r in rows(db, "SELECT evidence_ids FROM conflict") for j in json.loads(r["evidence_ids"])}
    return bool(ev & used), sorted(ev)


@check("D22", "detect", "customer-orders-erd.png (orders.total REAL, users.status) flagged stale")
def _(db):
    return has(conflicts(db, path="diagrams/customer-orders-erd.png"))


@check("D23", "detect", "wiki/accounts-http.md tells clients to call Accounts directly (vs gateway-only policy)")
def _(db):
    return has(conflicts(db, path="wiki/accounts-http.md", text=r"proxy|directly|accounts host|gateway|\bedge\b"))


@check("D24", "detect", "admin reads products directly from Stock, beyond ADR-005 (adjustments only)")
def _(db):
    return has(conflicts(db, path="apps/admin/src/app/index.tsx", text=r"ADR-005|exception|bypass"))


@check("D25", "detect", "logging-standard masking promise contradicted by Main.java (doc-vs-code conflict)")
def _(db):
    a = {c["id"] for c in conflicts(db, path="wiki/logging-standard.md")}
    b = {c["id"] for c in conflicts(db, path="apps/hr-service/src/Main.java")}
    return bool(a & b), sorted(a & b)


@check("D26", "detect", "gateway/order-service mint a fresh idempotency key per attempt (PAY-118 contributor)")
def _(db):
    return has([r for p in ("apps/api-gateway/src/index.ts", "apps/order-service/src/index.ts")
                for r in risks(db, path=p, text=r"idempot")])


@check("D27", "detect", "api-gateway owner Core Platform recorded from prose")
def _(db):
    return has(rows(db, "SELECT id FROM relation WHERE src = ? AND type = ? AND dst LIKE ?",
                    "comp:api-gateway", "OWNED_BY", "%core-platform%"))


@check("D28", "detect", "CODEOWNERS line 10: facilities-tracker owned by @northstar/workplace-tools")
def _(db):
    return has(rows(db, "SELECT id FROM relation WHERE src LIKE ? AND type = ? AND dst = ? AND status = ?",
                    "comp:%facilities-tracker", "OWNED_BY", "team:@northstar/workplace-tools", "observed"))


# ------------------------------------------------------------ MUST NOT DETECT / MUST NOT LINK
@check("N01", "not", "issues/ACC-42.md itself is not called stale (it describes the rename)")
def _(db):
    # citing ACC-42 as evidence of the CURRENT state is correct; flagging ACC-42 as the stale source is not
    rs = [r for r in conflicts(db, path="issues/ACC-42.md", kind="stale")
          if r["check_name"] != "llm-contradiction"
          or re.match(r"\W*(issues/)?ACC-42\b", r["description"], re.I)]
    return not rs, [r["id"] for r in rs]


@check("N02", "not", "no active relation conference-registration -> payment-service")
def _(db):
    rs = links(db, "comp:conference-registration", "comp:payment-service", statuses=("observed", "declared"))
    return not rs, [(r["id"], r["type"], r["status"], r["path"]) for r in rs]


@check("N03", "not", "impact(payment-service) does not list conference-registration")
def _(db):
    d = direct_dependents(db, "comp:payment-service")
    return "comp:conference-registration" not in d, sorted(d)


@check("N04", "not", "no active relation facilities-tracker -> inventory-service / api-gateway")
def _(db):
    src = [r["id"] for r in rows(db, "SELECT id FROM component WHERE id LIKE ?", "comp:%facilities-tracker")]
    rs = [x for s in src for d in ("comp:inventory-service", "comp:api-gateway")
          for x in links(db, s, d, statuses=("observed", "declared"))]
    return not rs, [(r["id"], r["type"], r["status"], r["path"]) for r in rs]


@check("N05", "not", "blog (journal /edge route, inventory of articles) not linked to commerce/people services")
def _(db):
    return not [1 for d in ("comp:api-gateway", "comp:inventory-service", "comp:user-service", "comp:hr-service")
                for _r in links(db, "comp:blog", d)], []


@check("N06", "not", "current checkout-sequence.png not flagged")
def _(db):
    rs = conflicts(db, path="diagrams/checkout-sequence.png")
    return not rs, [r["id"] for r in rs]


@check("N07", "not", "no LLM facts extracted from .agents/ files")
def _(db):
    n = rows(db, "SELECT count(*) n FROM evidence WHERE path LIKE ? AND extractor LIKE ?", ".agents/%", "llm:%")[0]["n"]
    return n == 0, n


@check("N08", "not", "decisions stated in ACC-42 are not marked superseded/rejected")
def _(db):
    rs = rows(db, "SELECT DISTINCT c.id FROM component c JOIN relation r ON r.src = c.id AND r.type = ? "
                  "JOIN evidence e ON e.id = r.evidence_id WHERE e.path = ? AND json_extract(c.props, ?) IN (?, ?)",
              "STATED_IN", "issues/ACC-42.md", "$.status", "superseded", "rejected")
    return not rs, [r["id"] for r in rs]


@check("N09", "not", "people-identity-workshop proposal not linked between real components")
def _(db):
    rs = rows(db, "SELECT r.id FROM relation r JOIN evidence e ON e.id = r.evidence_id WHERE e.path = ? "
                  "AND r.src LIKE ? AND r.dst LIKE ? AND r.status != ?",
              "diagrams/people-identity-workshop.png", "comp:%", "comp:%", "outdated")
    return not rs, [r["id"] for r in rs]


@check("N10", "not", "no self-loop dependency/data edges")
def _(db):
    tm = ",".join("?" * len(LINK))
    rs = rows(db, "SELECT id, src, type FROM relation WHERE src = dst AND type IN (%s)" % tm, *LINK)
    return not rs, [(r["id"], r["src"], r["type"]) for r in rs]


@check("N11", "not", "no rejected rule finding re-raised as llm-found (verdict contradiction)")
def _(db):
    n = verdict_contradictions(db)
    return not n, n


@check("N12", "not", "client apps linked only to their real backend in impact() (no stale/inverted prose edges)")
def _(db):
    bad = client_wrong_edges(db)
    return not bad, bad


@check("N13", "not", "impact() direct dependents contain no false components (vs code/manifests)")
def _(db):
    fp, _miss = impact_errors(db)
    return not fp, fp


# ------------------------------------------------------------ metric helpers
def _norm(s):
    return re.sub(r"[\s*_`#>|-]+", " ", s or "").strip().lower()


def verdict_contradictions(db):
    rule = rows(db, "SELECT id, evidence_ids, verdict FROM conflict WHERE check_name != ?", "llm-contradiction")
    llm = [set(json.loads(r["evidence_ids"])) for r in
           rows(db, "SELECT evidence_ids FROM conflict WHERE check_name = ?", "llm-contradiction")]
    return sorted(r["id"] for r in rule if r["verdict"] == "rejected"
                  and any(set(json.loads(r["evidence_ids"])) & ev for ev in llm))


def client_wrong_edges(db):
    out = []
    for client, ok in CLIENT_BACKENDS.items():
        deps = {d for d, cids in ((t, direct_dependents(db, t)) for t in
                                  [r["id"] for r in rows(db, "SELECT id FROM component WHERE kind = ?", "service")])
                if client in cids}
        out += [(client, d) for d in sorted(deps - ok)]
    return out


def impact_errors(db):
    fp, miss = [], []
    for cid, exp in EXPECTED_DEPENDENTS.items():
        got = direct_dependents(db, cid)
        fp += ["%s <- %s" % (cid, s) for s in sorted(got - exp)]
        miss += ["%s <- %s" % (cid, s) for s in sorted(exp - got)]
    return fp, miss


def metrics(db):
    m = {}

    def one(sql, *a):
        return rows(db, sql, *a)[0]["n"]
    m["conflicts_by_check_verdict"] = {"%s|%s" % (r["check_name"], r["v"]): r["n"] for r in rows(
        db, "SELECT check_name, coalesce(verdict, ?) v, count(*) n FROM conflict GROUP BY 1, 2", "none")}
    m["risks_by_rule_severity"] = {"%s|%s" % (r["rule"], r["severity"]): r["n"] for r in rows(
        db, "SELECT rule, severity, count(*) n FROM risk GROUP BY 1, 2")}
    m["decisions_by_kind_status"] = {"%s|%s" % (r["kind"], r["s"]): r["n"] for r in rows(
        db, "SELECT kind, json_extract(props, ?) s, count(*) n FROM component WHERE kind IN (?, ?, ?) GROUP BY 1, 2",
        "$.status", "decision", "policy", "constraint")}
    m["relations_by_type_status"] = {"%s|%s" % (r["type"], r["status"]): r["n"] for r in rows(
        db, "SELECT type, status, count(*) n FROM relation GROUP BY 1, 2")}
    for t in ("source", "component", "relation", "evidence", "claim", "conflict", "risk"):
        m["rows_" + t] = one("SELECT count(*) n FROM %s" % t)
    m["active_conflicts"] = one("SELECT count(*) n FROM conflict WHERE verdict IS NOT ?", "rejected")
    m["unverified_conflicts"] = one("SELECT count(*) n FROM conflict WHERE verdict IS NULL")
    rule = rows(db, "SELECT evidence_ids, verdict FROM conflict WHERE check_name != ?", "llm-contradiction")
    llm = [set(json.loads(r["evidence_ids"])) for r in
           rows(db, "SELECT evidence_ids FROM conflict WHERE check_name = ?", "llm-contradiction")]
    m["llm_conflicts_restating_rule"] = sum(1 for ev in llm if any(
        ev & set(json.loads(r["evidence_ids"])) and r["verdict"] != "rejected" for r in rule))
    m["verdict_contradictions"] = len(verdict_contradictions(db))
    cat = {"cors-any-origin": r"cors", "error-detail-leak": r"error (message|detail|text)|raw error|error\.message",
           "sensitive-data-in-logs": r"\blog"}
    rr = risks(db)
    m["llm_risks_restating_rule"] = sum(1 for x in rr if x["rule"] == "code-review-llm" and any(
        y["rule"] in cat and y["path"] == x["path"] and re.search(cat[y["rule"]], x["description"], re.I) for y in rr))
    doc = [x for x in rr if x["rule"] == "doc-reported-risk"]
    m["doc_reported_risks"] = len(doc)
    m["doc_risk_duplicate_quotes"] = len(doc) - len({x["quote"] for x in doc})
    m["doc_risks_offtopic"] = sum(1 for x in doc if not OFFTOPIC_KW.search(x["description"]))
    m["risk_fingerprint_collisions"] = len(rows(db, "SELECT fingerprint FROM risk GROUP BY 1 HAVING count(*) > 1"))
    code = [x for x in rr if x["rule"] == "code-review-llm"]
    m["code_risks"] = len(code)
    m["code_risk_trivial_quotes"] = sum(len(re.sub(r"[^A-Za-z0-9]", "", x["quote"])) < 8 for x in code)
    m["declared_only_dependency_edges"] = one(
        "SELECT count(*) n FROM relation r WHERE r.type IN (?, ?) AND r.status = ? AND r.src LIKE ? AND r.dst LIKE ? "
        "AND NOT EXISTS (SELECT 1 FROM relation o WHERE o.src = r.src AND o.dst = r.dst AND o.status = ? "
        "AND o.type IN (?, ?, ?, ?))", "DEPENDS_ON", "CALLS", "declared", "comp:%", "comp:%", "observed", *DEP)
    tm = ",".join("?" * len(LINK))
    m["self_loop_edges"] = one("SELECT count(*) n FROM relation WHERE src = dst AND type IN (%s)" % tm, *LINK)
    fp, miss = impact_errors(db)
    m["impact_false_dependents"], m["impact_missing_dependents"] = len(fp), len(miss)
    m["client_wrong_backend_edges"] = len(client_wrong_edges(db))
    m["conference_payment_links"] = len(links(db, "comp:conference-registration", "comp:payment-service",
                                              statuses=("observed", "declared")))
    m["superseded_from_historical_docs"] = one(
        "SELECT count(DISTINCT c.id) n FROM component c JOIN relation r ON r.src = c.id AND r.type = ? "
        "JOIN evidence e ON e.id = r.evidence_id JOIN source s ON s.path = e.path WHERE json_extract(c.props, ?) = ? "
        "AND s.doc_status = ? AND json_extract(c.props, ?) IS NULL",
        "STATED_IN", "$.status", "superseded", "historical", "$.supersedes")
    m["concept_components"] = one("SELECT count(*) n FROM component WHERE kind = ?", "concept")
    m["no_owner_findings"] = one("SELECT count(*) n FROM conflict WHERE check_name = ? AND verdict IS NOT ?",
                                 "no-owner", "rejected")
    m["cors_rule_findings"] = one("SELECT count(*) n FROM risk WHERE rule = ?", "cors-any-origin")
    m["observed_call_pairs"] = one("SELECT count(*) n FROM (SELECT DISTINCT src, dst FROM relation WHERE status = ? "
                                   "AND type IN (?, ?, ?))", "observed", *DEP[1:])
    text = {r["path"]: r["text"] or "" for r in rows(db, "SELECT path, text FROM source WHERE kind != ?", "diagram")}
    exact = near = found = total = triv = ctot = 0
    for e in rows(db, "SELECT path, line, quote, unit FROM evidence WHERE extractor LIKE ?", "llm:%"):
        if e["path"] not in text:
            continue
        if (e["unit"] or "").startswith("llm:code:"):
            ctot += 1
            triv += len(re.sub(r"[^A-Za-z0-9]", "", e["quote"])) < 8
            continue
        q = _norm(e["quote"])[:40]
        if not q or not e["line"]:
            continue
        lines = text[e["path"]].splitlines()
        win = [_norm(x) for x in lines[e["line"] - 1:e["line"] + 3]]
        total += 1
        found += q in _norm(text[e["path"]])
        exact += bool(win) and q[:20] in win[0]
        near += any(q[:20] in w for w in win)
    m["evidence_prose_rows"] = total
    m["evidence_quote_in_file_ratio"] = round(found / total, 4) if total else None
    m["evidence_prose_exact_line_ratio"] = round(exact / total, 4) if total else None
    m["code_evidence_rows"], m["code_evidence_trivial_quotes"] = ctot, triv
    return m


LOWER_IS_BETTER = ("unverified_conflicts", "llm_conflicts_restating_rule", "verdict_contradictions",
                   "llm_risks_restating_rule", "doc_risk_duplicate_quotes", "doc_risks_offtopic",
                   "risk_fingerprint_collisions", "code_risk_trivial_quotes", "code_evidence_trivial_quotes",
                   "declared_only_dependency_edges", "self_loop_edges", "impact_false_dependents",
                   "impact_missing_dependents", "client_wrong_backend_edges", "conference_payment_links",
                   "superseded_from_historical_docs")
HIGHER_IS_BETTER = ("evidence_quote_in_file_ratio", "evidence_prose_exact_line_ratio",
                    "no_owner_findings", "cors_rule_findings", "observed_call_pairs")
LLM_VARIANCE = {"unverified_conflicts", "llm_conflicts_restating_rule", "llm_risks_restating_rule",
                "doc_risk_duplicate_quotes", "doc_risks_offtopic", "code_risk_trivial_quotes",
                "code_evidence_trivial_quotes", "declared_only_dependency_edges", "client_wrong_backend_edges",
                "impact_false_dependents"}


def compare(a, b):
    bad = 0
    if [s["commit_id"] for s in a["stage_state"]] != [s["commit_id"] for s in b["stage_state"]]:
        print("WARNING: different source commits - not a like-for-like comparison")
    before = {c["id"]: c["ok"] for c in a["checks"]}
    for c in b["checks"]:
        if before.get(c["id"]) and not c["ok"]:
            bad += 1
            note = " (LLM-dependent: re-run once to confirm)" if c["id"] in LLM_DEPENDENT else ""
            print("REGRESSION %-22s %s%s  %s" % (c["id"], c["desc"], note, json.dumps(c["detail"], default=str)[:160]))
        elif before.get(c["id"]) is False and c["ok"]:
            print("improved   %-22s %s" % (c["id"], c["desc"]))
    for k in LOWER_IS_BETTER + HIGHER_IS_BETTER:
        x, y = a["metrics"].get(k), b["metrics"].get(k)
        if x is None or y is None or x == y:
            continue
        tol = max(1, round(0.1 * x)) if k in LLM_VARIANCE else 0
        worse = y > x + tol if k in LOWER_IS_BETTER else y < x - (0.01 if isinstance(x, float) else 0)
        bad += worse
        print("%-10s %-34s %s -> %s" % ("REGRESSION" if worse else "changed", k, x, y))
    for k in ("rows_conflict", "active_conflicts", "rows_risk", "doc_reported_risks", "code_risks",
              "rows_relation", "rows_claim", "concept_components"):
        if a["metrics"][k] != b["metrics"][k]:
            print("size       %-34s %s -> %s" % (k, a["metrics"][k], b["metrics"][k]))
    passed = sum(c["ok"] for c in b["checks"])
    print("checks: %d/%d pass (before %d/%d)" % (passed, len(b["checks"]), sum(c["ok"] for c in a["checks"]),
                                                len(a["checks"])))
    print("RESULT:", "FAIL (%d regressions)" % bad if bad else "OK - no regression")
    return 1 if bad else 0


def main(argv):
    if argv[:1] == ["--compare"]:
        with open(argv[1]) as f1, open(argv[2]) as f2:
            return compare(json.load(f1), json.load(f2))
    summary = "--summary" in argv
    argv = [x for x in argv if x != "--summary"]
    db = connect(argv[0] if argv else DEFAULT_DB)
    out = {"stage_state": rows(db, "SELECT stage, commit_id, signature, status FROM stage_state"),
           "checks": [], "metrics": metrics(db)}
    for cid, group, desc, fn in CHECKS:
        try:
            ok, detail = fn(db)
        except Exception as e:  # a broken check must show up, not crash the gate
            ok, detail = False, f"error: {e}"
        out["checks"].append({"id": cid, "group": group, "desc": desc, "ok": bool(ok), "detail": detail})
    if summary:
        for c in out["checks"]:
            print("%-4s %-22s %-6s %s  %s" % ("PASS" if c["ok"] else "FAIL", c["id"], c["group"], c["desc"],
                                              json.dumps(c["detail"], default=str)[:110]))
        for k, v in out["metrics"].items():
            if not isinstance(v, dict):
                print("  %-34s %s" % (k, v))
        print("checks: %d/%d pass" % (sum(c["ok"] for c in out["checks"]), len(out["checks"])))
    else:
        json.dump(out, sys.stdout, indent=1, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
