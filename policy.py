"""Policy checks: who may call whom directly, evaluated deterministically against observed calls.

Rules come from decisions the model extracts from documents (enrich.py stores an optional `check` on the
decision's STATED_IN relation). Two templates:

  require_mediation   actor must reach target through `via` for the listed operations
  permit_direct_call  actor may call target directly for the listed operations (an exception)

Only rules from ACCEPTED decisions are in force; proposed/superseded/rejected ones never create obligations,
and a decision of unknown status gives unknown, not a verdict. Operation scope is never widened: an empty
operation list is unknown scope, not "all". Each observed call (code-derived CALLS with a path) that a rule
names gets one disposition, and each in-force obligation without a direct call in scope gets a rule-level one:

  satisfied | violated | permitted_by_exception | unknown | not_applicable | conflicting_rules

Every assessment keeps the rules and evidence it was computed from and a reason in plain words. This explains
a computation over extracted premises; it does not prove the running system, and a model's reading of the
document can still be wrong (the rule's quote and status are shown so a reader can check it).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

EVALUATOR_VERSION = "2"  # bump when the semantics below change: stored assessments are then recomputed (2: one observation per call site)
TEMPLATES = ("require_mediation", "permit_direct_call")
OUTCOMES = ("satisfied", "violated", "permitted_by_exception", "unknown", "not_applicable", "conflicting_rules")
NOT_IN_FORCE = {"proposed", "superseded", "rejected"}
READS = {"GET", "HEAD", "OPTIONS"}
WRITES = {"POST", "PUT", "PATCH", "DELETE"}
OP_RE = re.compile(r"^(?:(all|reads|writes)(?::([\w./{}-]+))?|(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS|\*)\s+(/\S*))$", re.I)


def parse_operation(op) -> tuple | None:
    """'all' | 'reads' | 'writes' | 'reads:<word>' | 'writes:<word>' | '<METHOD> /path' -> parsed form, else None."""
    m = OP_RE.match(str(op or "").strip())
    if not m:
        return None
    if m.group(1):
        return ("class", m.group(1).lower(), (m.group(2) or "").lower() or None)
    return ("route", m.group(3).upper(), _norm(m.group(4)))


def _norm(path: str) -> str:
    segs = [("{}" if s.startswith((":", "{", "<", "$")) or "${" in s else s)
            for s in path.split("?", 1)[0].strip("/").split("/") if s]
    return "/" + "/".join(segs)


def _route_match(a: str, b: str) -> bool:
    sa, sb = a.strip("/").split("/"), b.strip("/").split("/")
    return len(sa) == len(sb) and all(x == y or "{}" in (x, y) for x, y in zip(sa, sb))


def op_applies(op: tuple, method: str, path: str) -> bool | None:
    """Does one parsed operation cover the call? None = cannot tell (e.g. the call's HTTP method is unknown)."""
    method = (method or "*").upper()
    if op[0] == "class":
        _, cls, word = op
        if word and word not in path.lower():
            return False
        if cls == "all":
            return True
        if method == "*":
            return None
        return method in (READS if cls == "reads" else WRITES)
    _, m, route = op
    if not _route_match(route, _norm(path)):
        return False
    if m == "*" or m == method:
        return True
    return None if method == "*" else False


@dataclass
class Rule:
    id: str             # decision/policy component id
    title: str
    source: str         # document that states it
    status: str         # decision status in that document
    ev: int
    template: str
    actors: list[str]
    target: str | None
    via: str | None
    operations: list[str]
    unresolved: list[str] = field(default_factory=list)

    def label(self) -> str:
        return f"'{self.title}' ({self.template}, {self.status}, {self.source})"

    def applies(self, method: str, path: str) -> bool | None:
        """True / False / None (unknown). An empty scope is unknown, never a wildcard."""
        parsed = [p for p in (parse_operation(o) for o in self.operations) if p]
        if not parsed:
            return None
        results = [op_applies(p, method, path) for p in parsed]
        return True if True in results else None if None in results else False


def load_rules(db) -> list[Rule]:
    rules = []
    for did, props, ev, dst, title in db.execute(
            "SELECT r.src, r.props, r.evidence_id, r.dst, c.name FROM relation r JOIN component c ON c.id = r.src "
            "WHERE r.type = 'STATED_IN' AND json_extract(r.props, '$.check') IS NOT NULL ORDER BY r.id"):
        p = json.loads(props)
        c = p["check"]
        rules.append(Rule(did, title, dst.removeprefix("source:"), p.get("decision_status") or "unknown", ev,
                          c["template"], list(c.get("actor") or []), c.get("target"), c.get("via"),
                          list(c.get("operations") or []), list(c.get("unresolved") or [])))
    return rules


def evaluate(st):
    """Write the assessments (and conflicts for violated / conflicting_rules) for the active store unit."""
    db = st.db
    rules = load_rules(db)
    if not rules:
        return
    calls = observed_calls(db)
    by_actor: dict[str, int] = {}
    for src, *_ in calls:
        by_actor[src] = by_actor.get(src, 0) + 1

    # one disposition per observed call that a rule names (actor and target both match)
    for src, dst, props, evs_call in calls:
        relevant = [r for r in rules if src in r.actors and r.target == dst]
        if not relevant:
            continue
        method, path = props.get("method") or "*", props["path"]
        outcome, reason = _dispose(relevant, method, path)
        subject = f"{src} -> {dst} {method} {path}"
        rule_ids = sorted({r.id for r in relevant})
        evs = evs_call + [r.ev for r in relevant]
        st.assessment("call", src, dst, subject, outcome, reason, rule_ids, evs)
        if outcome in ("violated", "conflicting_rules"):
            st.conflict("conflict", f"policy-{'violated' if outcome == 'violated' else 'conflict'}", subject,
                        f"{subject}: {reason}", 0.7, evs)

    # in-force obligations: is there any direct call in scope at all?
    for r in rules:
        if r.template != "require_mediation" or r.status != "accepted":
            continue
        for actor in r.actors or ["?"]:
            subject = f"{actor} -> {r.target or '?'} (rule {r.id})"
            if r.unresolved or not r.target or actor == "?" or not r.operations:
                why = (f"unresolved: {', '.join(r.unresolved)}" if r.unresolved
                       else "the document does not say which operations" if not r.operations else "no actor")
                st.assessment("rule", actor, r.target, subject, "unknown",
                              f"{r.label()} cannot be checked: {why}", [r.id], [r.ev])
                continue
            direct = [c for c in calls if c[0] == actor and c[1] == r.target
                      and r.applies(c[2].get("method"), c[2]["path"])]
            if direct:
                continue  # each such call already has its own disposition
            if by_actor.get(actor):
                via_calls = sum(1 for c in calls if c[0] == actor and c[1] == r.via)
                st.assessment("rule", actor, r.target, subject, "satisfied",
                              f"{r.label()}: none of the {by_actor[actor]} calls extracted from {actor} goes to "
                              f"{r.target} directly within scope ({via_calls} go to {r.via})", [r.id], [r.ev])
            else:
                st.assessment("rule", actor, r.target, subject, "unknown",
                              f"{r.label()}: no calls were extracted from {actor}'s code, so absence of direct "
                              "calls cannot be concluded", [r.id], [r.ev])


def observed_calls(db) -> list[tuple[str, str, dict, list[int]]]:
    """Code-derived calls with a path, one per call site: the rule-based extractor and the AI code pass may
    both report the same line (one with an unknown method, one with GET). Those are one observation: keep the
    known method and cite both pieces of evidence."""
    sites: dict[tuple, list] = {}
    for src, dst, props, ev, path, line in db.execute(
            "SELECT r.src, r.dst, r.props, r.evidence_id, e.path, e.line FROM relation r "
            "JOIN evidence e ON e.id = r.evidence_id WHERE r.type = 'CALLS' AND r.status = 'observed' "
            "AND json_extract(r.props, '$.path') IS NOT NULL ORDER BY r.id"):
        p = json.loads(props)
        key = (src, dst, _norm(p["path"]), path, line)
        if key not in sites:
            sites[key] = [src, dst, p, [ev]]
            continue
        site = sites[key]
        site[3].append(ev)
        if (site[2].get("method") or "*") == "*" and (p.get("method") or "*") != "*":
            site[2] = p  # the more specific reading of the same call
    return [tuple(v) for v in sites.values()]


def _dispose(relevant: list[Rule], method: str, path: str) -> tuple[str, str]:
    parts, obligations, exceptions, uncertain = [], [], [], []
    for r in relevant:
        if r.unresolved:
            parts.append(f"{r.label()} has unresolved arguments ({', '.join(r.unresolved)})")
            if r.status not in NOT_IN_FORCE:
                uncertain.append(r)
            continue
        hit = r.applies(method, path)
        scope = ", ".join(r.operations) or "unstated"
        if r.status in NOT_IN_FORCE:
            parts.append(f"{r.label()} is not in force")
        elif hit is False:
            parts.append(f"{r.label()} does not cover this operation (scope: {scope})")
        elif hit is None or r.status != "accepted":
            parts.append(f"{r.label()} may apply: " + ("scope cannot be matched" if hit is None
                                                      else "the decision's status is unknown") + f" (scope: {scope})")
            uncertain.append(r)
        elif r.template == "require_mediation":
            parts.append(f"{r.label()} requires going through {r.via} (scope: {scope})")
            obligations.append(r)
        else:
            parts.append(f"{r.label()} permits this direct call (scope: {scope})")
            exceptions.append(r)
    reason = "; ".join(parts)
    if len({r.via for r in obligations}) > 1:
        return "conflicting_rules", "in-force rules require different intermediaries: " + reason
    if obligations and exceptions:
        if all(r.operations == ["all"] for r in exceptions) and any("all" in r.operations for r in obligations):
            return "conflicting_rules", "an unscoped permission and an unscoped obligation cover the same call: " + reason
        return "permitted_by_exception", reason
    if obligations:
        if any(r.template == "permit_direct_call" for r in uncertain):
            return "unknown", "an exception might apply: " + reason
        return "violated", reason
    if exceptions:
        return "permitted_by_exception", reason + " (no in-force obligation found)"
    if uncertain:
        return "unknown", reason
    return "not_applicable", reason
