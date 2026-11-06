#!/usr/bin/env python3
"""Query the Context Service database built by ingest.py.

Every answer line is: claim  [confidence]  <- source:line "quote"

    python3 query.py context inventory-service [--focus redis] [--json]  # everything relevant, one call
    python3 query.py impact order-service     # what breaks if I change it
    python3 query.py component user-service   # everything known about it
    python3 query.py conflicts [topic]         # contradictions, gaps, stale docs
    python3 query.py risks [path-or-component] # security concerns + guidance
    python3 query.py search "minor units"      # full-text over all sources
    python3 query.py changes [run-id]          # new / resolved findings since the previous ingest
    python3 query.py decisions [component]     # ADRs, policies, constraints and whether they are in force
    python3 ask.py "free-form question"         # LLM agent over these tools (OpenRouter)
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

DB = Path(os.environ.get("CONTEXT_DB") or Path(__file__).resolve().parent / "context.db")
# edges along which a change propagates from dst back to src
DEPENDENCY_EDGES = ("DEPENDS_ON", "CALLS", "CONFIGURED_TO_CALL", "DEPLOY_DEPENDS_ON")
# a document's plan or open proposal is context, not a dependency that exists today
NOT_PLANNED = "IFNULL(json_extract(props, '$.perspective'), '') NOT IN ('intended', 'proposed')"


def connect() -> sqlite3.Connection:
    if not DB.exists():
        sys.exit("context.db not found - run: python3 ingest.py")
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'source'").fetchone():
        sys.exit("context.db is not initialised - run: python3 ingest.py")
    return db


def candidates(db, name: str) -> list[tuple[int, str, str]]:
    """(rank, component id, why it matches), best first. Rank: 0 exact id, 1 project/service name,
    2 another component's name (table, team, ...) or a name a document gives a component (KNOWN_AS alias),
    3 a placeholder concept the model created for a name it could not map. Several ids in the best rank =
    ambiguous. Within a rank, projects/services come first."""
    name = name.strip()
    found: dict[str, tuple[int, str]] = {}

    def add(rank, cid, why):
        if cid not in found or rank < found[cid][0]:
            found[cid] = (rank, why)
    for (cid,) in db.execute("SELECT id FROM component WHERE id = ?", (name,)):
        add(0, cid, "id")
    for (cid,) in db.execute("SELECT id FROM component WHERE lower(name) = lower(?) OR id LIKE ?", (name, f"%:{name}")):
        add(1 if cid.startswith("comp:") else 3 if cid.startswith("concept:") else 2, cid, "name")
    for cid, path in db.execute(
            "SELECT DISTINCT src, substr(dst, 8) FROM relation WHERE type = 'KNOWN_AS' "
            "AND lower(json_extract(props, '$.alias')) = lower(?) ORDER BY src, dst", (name,)):
        add(2, cid, f"called '{name}' in {path}")
    return sorted(((r, cid, why) for cid, (r, why) in found.items()),
                  key=lambda c: (c[0], not c[1].startswith("comp:"), len(c[1]), c[1]))


def resolve(db, name: str) -> str:
    found = candidates(db, name)
    if not found:
        sys.exit(f"unknown component: {name}")
    return found[0][1]


def cite(db, ev_id: int) -> str:
    e = db.execute("SELECT path, line, quote FROM evidence WHERE id = ?", (ev_id,)).fetchone()
    loc = f"{e['path']}:{e['line']}" if e["line"] else e["path"]
    return f'{loc}  "{e["quote"][:90]}"'


def dependents(db, node: str, observed: bool = True) -> list[tuple[str, list]]:
    """Components that depend on `node`, grouped by source: [(src, relations)].
    observed=True: code/config evidence only (what really breaks). observed=False: edges that only
    documents or diagrams claim (shown separately, never mixed into the impact chain)."""
    marks = ",".join("?" * len(DEPENDENCY_EDGES))
    cond = "status = 'observed'" if observed else f"status = 'declared' AND {NOT_PLANNED}"
    rows = db.execute(f"SELECT * FROM relation WHERE dst = ? AND src != dst AND type IN ({marks}) AND {cond} "
                      "ORDER BY confidence DESC, id", (node, *DEPENDENCY_EDGES)).fetchall()
    by_src: dict[str, list] = {}
    for r in rows:
        by_src.setdefault(r["src"], []).append(r)
    return list(by_src.items())


def impact(db, name: str, limit: int = 6):
    """Summary first (owner, data, dependents), then the top decisions, conflicts and risks.
    Sections are capped at `limit` items, so the most important facts survive output limits."""
    root = resolve(db, name)
    short = root.split(":", 1)[-1]
    owners = db.execute("SELECT DISTINCT dst FROM relation WHERE src = ? AND type = 'OWNED_BY'", (root,)).fetchall()
    tables = db.execute("SELECT dst FROM relation WHERE src = ? AND type = 'HAS_TABLE'", (root,)).fetchall()
    print(f"Changing {root}")
    print("  Owner:", ", ".join(o["dst"] for o in owners) or "NONE (gap)")
    if tables:
        print("  Owns data:", ", ".join(t["dst"] for t in tables))

    # dependents, hop by hop (code/config evidence only; documented-only edges are listed separately)
    seen, frontier, depth, layers = {root}, [root], 0, []
    while frontier:
        depth += 1
        nxt, layer = [], []
        for node in frontier:
            for src, rels in dependents(db, node):
                if src in seen:
                    continue
                seen.add(src)
                nxt.append(src)
                layer.append((src, node, rels))
        if layer:
            layers.append(layer)
        frontier = nxt
    direct = [s for s, _, _ in layers[0]] if layers else []
    indirect = [s for layer in layers[1:] for s, _, _ in layer]
    print(f"  Affected: {len(direct)} direct dependents, {len(indirect)} transitive")

    if layers:
        print("\n  Direct dependents (one line each; use component(<name>) for every piece of evidence):")
        for src, _, rels in layers[0]:
            types = sorted({r["type"] for r in rels})
            paths = sorted({json.loads(r["props"] or "{}").get("path") for r in rels} - {None})
            via = f" via {', '.join(paths[:3])}" + (" ..." if len(paths) > 3 else "") if paths else ""
            print(f"  {src:<26} {'+'.join(types)}{via}  [{max(r['confidence'] for r in rels)}]")
            print(f"      <- {cite(db, rels[0]['evidence_id'])}")
        for i, layer in enumerate(layers[1:], 2):
            print(f"  Hop {i}: " + ", ".join(f"{s} (via {n})" for s, n, _ in layer))
    documented = [(s, rels) for s, rels in dependents(db, root, observed=False) if s not in seen]
    if documented:
        print("\n  Documented only (a doc or diagram claims it; no code/config evidence - may be stale or wrong):")
        for src, rels in documented[:limit]:
            print(f"  {src:<26} {rels[0]['type']}  [{rels[0]['confidence']}]  <- {cite(db, rels[0]['evidence_id'])}")

    if db.execute("SELECT 1 FROM relation WHERE dst = ? AND type = 'GOVERNS' LIMIT 1", (root,)).fetchone():
        decisions(db, root, header="\n  Decisions & policies that govern it:", limit=limit)
    found = related_findings(db, root)
    for item, header in (("conflict", "\n  Known conflicts touching it:"), ("risk", "\n  Security concerns to not repeat:")):
        rows = [(r, why) for kind, r, why in found if kind == item]
        if rows:
            print(header)
        for r, why in rows[:limit]:
            if item == "conflict":
                tag = f" {r['verdict']}" if r["verdict"] else ""
                print(f"  [{r['kind'].upper()} {r['confidence']}{tag}] {r['description']}  ({why})")
                if r["rationale"]:
                    print(f"      verifier: {r['rationale']}")
                for ev in json.loads(r["evidence_ids"])[:3]:
                    print(f"      <- {cite(db, ev)}")
            else:
                print(f"  [{r['severity'].upper()} {r['confidence']:.2f}] {r['rule']}: {r['description']}  ({why})\n"
                      f"      do instead: {r['guidance']}\n      <- {cite(db, r['evidence_id'])}")
        _more(len(rows), min(len(rows), limit), f"call context('{short}') or {item}s('{short}')")
    docs = db.execute("SELECT DISTINCT dst FROM relation WHERE src = ? AND type = 'MENTIONED_IN'", (root,)).fetchall()
    if docs:
        names = [d["dst"].removeprefix("source:") for d in docs]
        print(f"\n  Docs that mention it ({len(names)}): " + ", ".join(names[:12]) + (" ..." if len(names) > 12 else ""))


def component(db, name: str):
    cid = resolve(db, name)
    c = db.execute("SELECT * FROM component WHERE id = ?", (cid,)).fetchone()
    print(f"{cid}  ({c['kind']}, {c['path'] or '-'})  {c['props'] or ''}\n")
    for direction, col, other in (("outgoing", "src", "dst"), ("incoming", "dst", "src")):
        rows = db.execute(f"SELECT * FROM relation WHERE {col} = ? AND type != 'MENTIONED_IN' "
                          "ORDER BY type, confidence DESC", (cid,)).fetchall()
        if rows:
            print(f"{direction}:")
            for r in rows:
                arrow = f"{r['type']} -> {r[other]}" if direction == "outgoing" else f"{r[other]} {r['type']} ->"
                print(f"  {arrow}  ({r['status']}) [{r['confidence']}]\n      <- {cite(db, r['evidence_id'])}")


def _more(total: int, shown: int, hint: str):
    if total > shown:
        print(f"  ... {total - shown} more - {hint}")


def conflicts(db, topic: str | None = None, header: str | None = None, limit: int | None = None):
    q = "SELECT * FROM conflict WHERE IFNULL(verdict, '') != 'rejected'"
    args = ()
    if topic:
        q += " AND (subject LIKE ? OR description LIKE ?)"
        args = (f"%{topic}%", f"%{topic}%")
    rows = db.execute(q + " ORDER BY confidence DESC", args).fetchall()
    if header and rows:
        print(header)
    total, rows = len(rows), rows[:limit] if limit else rows
    for c in rows:
        tag = f" {c['verdict']}" if c["verdict"] else ""
        print(f"  [{c['kind'].upper()} {c['confidence']}{tag}] {c['description']}")
        if c["rationale"]:
            print(f"      verifier: {c['rationale']}")
        for ev in json.loads(c["evidence_ids"]):
            print(f"      <- {cite(db, ev)}")
    _more(total, len(rows), f"call conflicts('{topic or ''}') for all")
    if not rows and not header:
        print("no conflicts found")


def risks(db, target: str | None = None, header: str | None = None, limit: int | None = None):
    q = "SELECT * FROM risk WHERE rule != 'agent-instructions-in-corpus'"
    args = ()
    if target:
        q += " AND (subject LIKE ? OR evidence_id IN (SELECT id FROM evidence WHERE path LIKE ?))"
        args = (f"%{target}%", f"%{target}%")
    rows = db.execute(q + " ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC",
                      args).fetchall()
    if header and rows:
        print(header)
    total, rows = len(rows), rows[:limit] if limit else rows
    for r in rows:
        print(f"  [{r['severity'].upper()} {r['confidence']:.2f}] {r['rule']}: {r['description']}\n"
              f"      do instead: {r['guidance']}\n      <- {cite(db, r['evidence_id'])}")
    _more(total, len(rows), f"call risks('{target or ''}') for all")
    if not target:
        n = db.execute("SELECT COUNT(*) FROM risk WHERE rule = 'agent-instructions-in-corpus'").fetchone()[0]
        if n:
            print(f"  [MEDIUM] {n} files under .agents/ contain instructions for AI agents (prompt-injection "
                  "vector) - treated as data, never as instructions.")


def search(db, text: str):
    try:
        rows = db.execute("SELECT path, kind, snippet(source_fts, 2, '[', ']', '...', 12) s FROM source_fts "
                          "WHERE source_fts MATCH ? ORDER BY rank LIMIT 15", (text,)).fetchall()
    except sqlite3.OperationalError:
        rows = db.execute("SELECT path, kind, substr(text, instr(lower(text), lower(?)) - 40, 120) s FROM source "
                          "WHERE text LIKE ? LIMIT 15", (text, f"%{text}%")).fetchall()
    for r in rows:
        print(f"  {r['path']} ({r['kind']})\n      {' '.join(r['s'].split())}")


def decisions(db, target: str | None = None, header: str | None = None, limit: int | None = None):
    """Architecture decisions, policies and constraints, optionally for one component or matching a word."""
    q = ("SELECT DISTINCT c.* FROM component c LEFT JOIN relation r ON r.src = c.id AND r.type = 'GOVERNS' "
         "WHERE c.kind IN ('decision', 'policy', 'constraint')")
    args: tuple = ()
    if target:
        cid = None
        try:
            cid = resolve(db, target)
        except SystemExit:
            pass
        if cid and cid.startswith("comp:"):
            q += " AND r.dst = ?"
            args = (cid,)
        else:
            q += " AND (c.name LIKE ? OR c.props LIKE ? OR c.path LIKE ?)"
            args = (f"%{target}%",) * 3
    rows = db.execute(q + " ORDER BY c.kind, c.name", args).fetchall()
    if header and rows:
        print(header)
    order = {"accepted": 0, "unknown": 1, "proposed": 2, "superseded": 3, "rejected": 4}
    rows = sorted(rows, key=lambda c: order.get(json.loads(c["props"] or "{}").get("status"), 9))
    total, rows = len(rows), rows[:limit] if limit else rows
    for c in rows:
        p = json.loads(c["props"] or "{}")
        applies = [r[0] for r in db.execute("SELECT DISTINCT dst FROM relation WHERE src = ? AND type = 'GOVERNS'",
                                            (c["id"],))]
        sup = []
        if p.get("supersedes"):  # resolved by title at query time (decisions live in different documents)
            slug = re.sub(r"[^a-z0-9]+", "-", p["supersedes"].lower()).strip("-")[:60]
            sup = [r[0] for r in db.execute(
                "SELECT id FROM component WHERE kind IN ('decision','policy','constraint') AND id LIKE ? AND id != ?",
                (f"%:{slug}", c["id"]))] or [f"'{p['supersedes']}' (not found)"]
        note = " - IN FORCE" if p.get("status") == "accepted" else " - intent only, not approved" \
            if p.get("status") == "proposed" else " - NO LONGER IN FORCE" if p.get("status") in {"superseded", "rejected"} else ""
        print(f"  [{c['kind'].upper()} {p.get('status', '?')}{note}] {c['name']}: {p.get('rule')}")
        if applies:
            print(f"      applies to: {', '.join(applies[:6])}")
        if sup:
            print(f"      supersedes: {', '.join(sup)}")
        stated = db.execute("SELECT evidence_id FROM relation WHERE src = ? AND type IN ('STATED_IN', 'GOVERNS') "
                            "ORDER BY type = 'GOVERNS', id LIMIT 1", (c["id"],)).fetchone()
        if stated:
            print(f"      <- {cite(db, stated[0])}")
    _more(total, len(rows), f"call decisions('{target or ''}') for all")
    if not rows and not header:
        print("no decisions or policies found")


def changes(db, run: str | None = None):
    """What appeared / disappeared in a run compared with its baseline (default: latest run)."""
    runs = db.execute("SELECT * FROM run ORDER BY id").fetchall()
    if not runs:
        print("no runs recorded")
        return
    print("run history:")
    for r in runs[-8:]:
        base = f"vs #{r['baseline_run']}: +{r['new']} new, -{r['resolved']} resolved" if r["baseline_run"] else "baseline"
        print(f"  #{r['id']}  {r['finished']}  commit {r['commit_sha']}  llm={'on' if r['llm'] else 'off'}  "
              f"{r['conflicts']} conflicts, {r['risks']} risks  ({base})")
    target = int(run) if run and run.isdigit() else runs[-1]["id"]
    rows = db.execute("SELECT * FROM change WHERE run_id = ? ORDER BY change, item", (target,)).fetchall()
    print(f"\nchanges in run #{target}:" if rows else f"\nno changes recorded for run #{target}")
    latest = target == runs[-1]["id"]
    total, rows = len(rows), rows[:30]
    for c in rows:
        sign = "+" if c["change"] == "new" else "-"
        print(f"  {sign} {c['change'].upper()} {c['item']}: {c['summary']}")
        if latest and c["ref_id"]:
            table = "conflict" if c["item"] == "conflict" else "risk"
            row = db.execute(f"SELECT * FROM {table} WHERE id = ?", (c["ref_id"],)).fetchone()
            evs = json.loads(row["evidence_ids"]) if table == "conflict" else [row["evidence_id"]]
            for ev in evs[:2]:
                print(f"      <- {cite(db, ev)}")
    _more(total, len(rows), "query the change table with sql for the rest")


# --------------------------------------------------------------------------- findings of one component

def names_component(subject: str, short: str) -> bool:
    """Does a finding's subject name this component? Whole names only: 'order-service' matches
    'order-service.orders.total' and 'api-gateway -> order-service', not 'order-service-legacy' or 'ui' in 'build'."""
    subject = subject or ""
    if subject in (short, f"comp:{short}"):
        return True
    for part in re.split(r"\s*->\s*|[\s,;()]+", subject):
        part = part.removeprefix("comp:")
        if part == short or part.startswith(short + "."):
            return True
    return False


def related_findings(db, cid: str) -> list[tuple[str, sqlite3.Row, str]]:
    """Conflicts and risks of one component, each with the basis of the link: the finding's subject names
    the component, the finding is about it (verifier), or its evidence lies inside the component's folder."""
    short = cid.split(":", 1)[-1]
    row = db.execute("SELECT path FROM component WHERE id = ?", (cid,)).fetchone()
    folder = row["path"] if row and row["path"] and row["path"] not in (".", "") and cid.startswith("comp:") else None
    paths = {r[0]: r[1] for r in db.execute("SELECT id, path FROM evidence")}

    def basis(subject: str, evs: list[int]) -> str | None:
        if subject == cid or names_component(subject, short):
            return "subject names it"
        if folder and any((paths.get(e) or "").startswith(folder + "/") for e in evs):
            return f"evidence in {folder}/"
        return None
    out = []
    for c in db.execute("SELECT * FROM conflict WHERE IFNULL(verdict, '') != 'rejected' ORDER BY confidence DESC, id"):
        why = basis(c["subject"], json.loads(c["evidence_ids"]))
        if why:
            out.append(("conflict", c, why))
    for r in db.execute("SELECT * FROM risk WHERE rule != 'agent-instructions-in-corpus' ORDER BY CASE severity "
                        "WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC, id"):
        why = basis(r["subject"] or "", [r["evidence_id"]])
        if why:
            out.append(("risk", r, why))
    return out


# --------------------------------------------------------------------------- context: one call, all of it

PROSE_EXT = (".md", ".mdx", ".rst", ".txt", ".adoc")
CONTEXT_SCHEMA = "0.1"
STATUS_ORDER = {"observed": 0, "declared": 1, "outdated": 2}
# what an engineer needs first: ownership, interfaces and who talks to whom, data; libraries last
TYPE_ORDER = {t: i for i, t in enumerate((
    "OWNED_BY", "CALLS", "CONFIGURED_TO_CALL", "DEPLOY_DEPENDS_ON", "DEPENDS_ON", "HAS_TABLE", "READS", "WRITES",
    "EXPOSES", "DECLARES", "PUBLISHES_EVENT", "CONSUMES_EVENT", "DIAGRAM_EDGE"))}


def _passage(text: str | None, line: int | None, path: str) -> dict | None:
    """The cited line expanded to its paragraph (docs) or a few lines of code, with the nearest heading."""
    if not text or not line:
        return None
    lines = text.splitlines()
    if not 1 <= line <= len(lines):
        return None
    i = line - 1
    if path.lower().endswith(PROSE_EXT):
        a = i
        while a > 0 and lines[a - 1].strip() and not lines[a].lstrip().startswith("#") and i - a < 6:
            a -= 1
        b = i + 1
        while b < len(lines) and lines[b].strip() and not lines[b].lstrip().startswith("#") and b - a < 12:
            b += 1
    else:
        a, b = max(0, i - 2), min(len(lines), i + 3)
    heading = None
    if path.lower().endswith(PROSE_EXT):  # '#' starts a comment, not a heading, in code and config
        heading = next((lines[k].lstrip("# ").strip() for k in range(i, -1, -1)
                        if lines[k].lstrip().startswith("#")), None)
    return {"start": a + 1, "end": b, "heading": heading, "text": "\n".join(lines[a:b])}


def _focus_first(items: list, focus: str | None, text_of) -> list:
    """Focus only changes the order (matching items first); it never removes anything."""
    if not focus:
        return items
    f = focus.lower()
    return sorted(items, key=lambda it: f not in text_of(it).lower())


def component_context(db, name: str, focus: str | None = None, max_items: int = 8, passages: bool = True) -> dict:
    """Deterministic evidence package for one component (no model calls). Callers interpret it."""
    found = candidates(db, name)
    if not found:
        return {"schema_version": CONTEXT_SCHEMA, "candidates": [],
                "error": f"unknown component: {name}. Names that documents use (e.g. product or code names) are "
                         f"recorded by model enrichment; try search(\"{name}\") for the raw text."}
    best = [c for c in found if c[0] == found[0][0]]
    if len({cid for _, cid, _ in best}) > 1:
        return {"schema_version": CONTEXT_SCHEMA, "ambiguous": name,
                "candidates": [{"id": cid, "matched_by": why} for _, cid, why in found[:10]]}
    cid, matched_by = best[0][1], best[0][2]
    c = db.execute("SELECT * FROM component WHERE id = ?", (cid,)).fetchone()
    omitted: dict[str, int] = {}
    evidence_ids: list[int] = []

    def cap(section: str, items: list) -> list:
        if len(items) > max_items:
            omitted[section] = len(items) - max_items
        kept = items[:max_items]
        for it in kept:
            evidence_ids.extend(it.get("evidence", []))
        return kept

    # relationships, implemented (observed) apart from documented and outdated ones
    rels = []
    for r in db.execute("SELECT * FROM relation WHERE (src = ? OR dst = ?) AND type NOT IN "
                        "('MENTIONED_IN', 'STATED_IN', 'KNOWN_AS', 'GOVERNS') ORDER BY id", (cid, cid)):
        props = json.loads(r["props"] or "{}")
        out = r["src"] == cid
        rels.append({"id": r["id"], "direction": "out" if out else "in", "type": r["type"],
                     "other": r["dst"] if out else r["src"], "status": r["status"], "confidence": r["confidence"],
                     "perspective": props.get("perspective"), "method": props.get("method"), "path": props.get("path"),
                     "evidence": [r["evidence_id"]]})
    rels.sort(key=lambda x: (STATUS_ORDER.get(x["status"], 3), TYPE_ORDER.get(x["type"], len(TYPE_ORDER)),
                             -x["confidence"], x["id"]))
    rels = _focus_first(rels, focus, lambda x: f"{x['type']} {x['other']} {x['path'] or ''}")

    decisions_ = []
    for r in db.execute("SELECT c.id, c.kind, c.name, c.props, r.evidence_id FROM relation r JOIN component c "
                        "ON c.id = r.src WHERE r.dst = ? AND r.type = 'GOVERNS' ORDER BY r.id", (cid,)):
        p = json.loads(r["props"] or "{}")
        decisions_.append({"id": r["id"], "kind": r["kind"], "title": r["name"], "status": p.get("status"),
                           "rule": p.get("rule"), "date": p.get("date"), "evidence": [r["evidence_id"]]})
    order = {"accepted": 0, "unknown": 1, "proposed": 2, "superseded": 3, "rejected": 4}
    decisions_.sort(key=lambda d: order.get(d["status"], 9))
    decisions_ = _focus_first(decisions_, focus, lambda d: f"{d['title']} {d['rule'] or ''}")

    has_persp = "perspective" in {r[1] for r in db.execute("PRAGMA table_info(claim)")}
    claims = []
    for r in db.execute("SELECT * FROM claim WHERE component = ? ORDER BY doc_date DESC, id", (cid,)):
        claims.append({"id": r["id"], "topic": r["topic"], "text": r["text"], "path": r["path"],
                       "doc_date": r["doc_date"], "doc_status": r["doc_status"],
                       "perspective": (r["perspective"] if has_persp else None) or "unknown",
                       "evidence": [r["evidence_id"]]})
    claims = _focus_first(claims, focus, lambda x: f"{x['topic'] or ''} {x['text']}")

    findings = []
    for kind, r, why in related_findings(db, cid):
        if kind == "conflict":
            findings.append({"item": "conflict", "id": r["id"], "kind": r["kind"], "check": r["check_name"],
                             "description": r["description"], "confidence": r["confidence"], "verdict": r["verdict"],
                             "rationale": r["rationale"], "linked_by": why, "evidence": json.loads(r["evidence_ids"])})
        else:
            findings.append({"item": "risk", "id": r["id"], "rule": r["rule"], "severity": r["severity"],
                             "description": r["description"], "guidance": r["guidance"],
                             "confidence": r["confidence"], "linked_by": why, "evidence": [r["evidence_id"]]})
    findings = _focus_first(findings, focus, lambda x: f"{x.get('check') or x.get('rule')} {x['description']}")

    # policy assessments where it is the caller or the callee; problems first
    rank = {"violated": 0, "conflicting_rules": 1, "unknown": 2, "permitted_by_exception": 3, "satisfied": 4,
            "not_applicable": 5}
    assessments = []
    if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'assessment'").fetchone():
        for r in db.execute("SELECT * FROM assessment WHERE actor = ? OR target = ? ORDER BY id", (cid, cid)):
            assessments.append({"id": r["id"], "scope": r["scope"], "subject": r["subject"], "outcome": r["outcome"],
                                "reason": r["reason"], "rules": json.loads(r["rules"]),
                                "evidence": json.loads(r["evidence_ids"])})
    assessments.sort(key=lambda a: (rank.get(a["outcome"], 9), a["id"]))
    assessments = _focus_first(assessments, focus, lambda a: f"{a['subject']} {a['reason']}")

    aliases = [{"alias": json.loads(r["props"] or "{}").get("alias"), "evidence": [r["evidence_id"]]}
               for r in db.execute("SELECT props, evidence_id FROM relation WHERE src = ? AND type = 'KNOWN_AS' "
                                   "ORDER BY id", (cid,))]
    docs = [r[0].removeprefix("source:") for r in db.execute(
        "SELECT DISTINCT dst FROM relation WHERE src = ? AND type = 'MENTIONED_IN' ORDER BY dst", (cid,))]

    result = {
        "schema_version": CONTEXT_SCHEMA,
        "component": {"id": cid, "kind": c["kind"], "name": c["name"], "path": c["path"],
                      "props": json.loads(c["props"] or "{}"), "matched_by": matched_by},
        "snapshot": _snapshot(db),
        "aliases": cap("aliases", aliases),
        "relationships": cap("relationships", rels),
        "decisions": cap("decisions", decisions_),
        "claims": cap("claims", claims),
        "findings": cap("findings", findings),
        "policy_assessments": cap("policy_assessments", assessments),
        "mentioned_in": docs[:max_items * 2],
    }
    if len(docs) > max_items * 2:
        omitted["mentioned_in"] = len(docs) - max_items * 2
    result["evidence"] = _evidence_pack(db, evidence_ids, passages)
    result["unknowns"] = _unknowns(db, cid, c, rels, claims, result["snapshot"])
    result["coverage"] = {"focus": focus, "focus_rule": "focus only puts matching items first; nothing is hidden",
                          "findings_linked_by": "the finding's subject names the component, or its evidence lies "
                                                "inside the component's folder (basis given per finding)"}
    result["truncation"] = {"truncated": bool(omitted) or not passages, "omitted_counts": omitted,
                            "passages": "included" if passages else "omitted to fit the size budget"}
    return result


def _snapshot(db) -> dict:
    snap = {}
    if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'stage_state'").fetchone():
        for r in db.execute("SELECT stage, commit_id, status, detail FROM stage_state"):
            snap[r["stage"]] = {"commit": r["commit_id"], "status": r["status"], "detail": r["detail"]}
    return snap


def _evidence_pack(db, ids: list[int], passages: bool) -> list[dict]:
    out = []
    for ev in dict.fromkeys(ids):  # dedupe, keep first-use order
        e = db.execute("SELECT e.*, s.text AS src_text, s.kind AS src_kind, s.doc_date, s.doc_status, s.blob "
                       "FROM evidence e LEFT JOIN source s ON s.path = e.path WHERE e.id = ?", (ev,)).fetchone()
        if not e:
            continue
        item = {"id": e["id"], "path": e["path"], "line": e["line"], "quote": e["quote"], "extractor": e["extractor"],
                "source": {"kind": e["src_kind"], "doc_date": e["doc_date"], "doc_status": e["doc_status"],
                           "blob": e["blob"]}}
        if passages:
            p = _passage(e["src_text"], e["line"], e["path"])
            if p and p["text"].strip() != e["quote"].strip():
                item["passage"] = p
        out.append(item)
    return out


def _unknowns(db, cid, c, rels, claims, snap) -> list[str]:
    """Only recorded gaps and coverage limits; nothing is inferred."""
    out = []
    if cid.startswith("comp:") and not any(r["type"] == "OWNED_BY" for r in rels):
        out.append("no OWNED_BY relation is recorded for it")
    if not rels:
        out.append("no relationships are recorded for it")
    model = snap.get("model")
    if model and model["status"] != "complete":
        out.append(f"model stage incomplete at {(model['commit'] or '-')[:12]} ({model['detail'] or 'no detail'}): "
                   "document claims, decisions and aliases may be missing")
    elif not claims:
        out.append("no document claims are linked to it")
    return out


def render_context(ctx: dict) -> str:
    """Compact text for people and inexpensive models: one line per item, evidence once at the end (E<id>)."""
    if "error" in ctx:
        return ctx["error"]
    if "ambiguous" in ctx:
        lines = [f"'{ctx['ambiguous']}' is ambiguous; call context with one of these ids:"]
        lines += [f"  {c['id']}  ({c['matched_by']})" for c in ctx["candidates"]]
        return "\n".join(lines)
    comp, L = ctx["component"], []
    snap = ctx["snapshot"]
    stages = "; ".join(f"{k} {v['status']} @{(v['commit'] or '-')[:12]}" for k, v in sorted(snap.items()))
    L.append(f"{comp['id']} ({comp['kind']}, {comp['path'] or '-'})  matched by {comp['matched_by']}")
    if stages:
        L.append(f"snapshot: {stages}")
    refs = lambda it: " ".join(f"E{e}" for e in it["evidence"])
    if ctx["aliases"]:
        L.append("also called: " + ", ".join(f"{a['alias']} ({refs(a)})" for a in ctx["aliases"]))
    for status, title in (("observed", "IMPLEMENTED (code/config)"), ("declared", "DOCUMENTED (specs/docs, may be stale)"),
                          ("outdated", "OUTDATED (archive/history)")):
        rows = [r for r in ctx["relationships"] if r["status"] == status]
        if rows:
            L.append(title + ":")
        for r in rows:
            via = " ".join(x for x in (r["method"] if r["method"] not in (None, "*") else None, r["path"]) if x)
            arrow = f"{r['type']} -> {r['other']}" if r["direction"] == "out" else f"{r['other']} {r['type']} -> it"
            persp = f" <{r['perspective']}>" if r["perspective"] and r["perspective"] != "current" else ""
            L.append(f"  {arrow}{' ' + via if via else ''}{persp} [{r['confidence']}] {refs(r)}")
    if ctx["decisions"]:
        L.append("DECISIONS / POLICIES governing it:")
    for d in ctx["decisions"]:
        L.append(f"  [{d['kind']} {d['status']}] {d['title']}: {d['rule']} {refs(d)}")
    if ctx["claims"]:
        L.append("DOCUMENT CLAIMS (<how the sentence presents it> path, date, document status):")
    for x in ctx["claims"]:
        L.append(f"  <{x['perspective']}> {x['text']}  ({x['path']}, {x['doc_date'] or 'undated'}, {x['doc_status']}) "
                 f"{refs(x)}")
    if ctx["findings"]:
        L.append("FINDINGS:")
    for f in ctx["findings"]:
        if f["item"] == "conflict":
            v = f" {f['verdict']}" if f["verdict"] else ""
            L.append(f"  [{f['kind'].upper()} {f['confidence']}{v}] {f['description']} {refs(f)}  (linked: {f['linked_by']})")
            if f["rationale"]:
                L.append(f"      verifier: {f['rationale']}")
        else:
            L.append(f"  [RISK {f['severity']} {f['confidence']:.2f}] {f['rule']}: {f['description']}; do instead: "
                     f"{f['guidance']} {refs(f)}  (linked: {f['linked_by']})")
    if ctx["policy_assessments"]:
        L.append("POLICY ASSESSMENTS (call rules from decisions vs observed calls):")
    for a in ctx["policy_assessments"]:
        L.append(f"  [{a['outcome'].upper()}] {a['subject']}: {a['reason']} {refs(a)}")
    if ctx["mentioned_in"]:
        L.append("mentioned in: " + ", ".join(ctx["mentioned_in"]))
    if ctx["unknowns"]:
        L.append("UNKNOWN / NOT RECORDED:")
        L += [f"  - {u}" for u in ctx["unknowns"]]
    if ctx["evidence"]:
        L.append("EVIDENCE (cite as path:line):")
    for e in ctx["evidence"]:
        src = e["source"]
        meta = ", ".join(x for x in (src["kind"], src["doc_date"], src["doc_status"]) if x)
        p = e.get("passage")
        loc = f"{e['path']}:{p['start']}-{p['end']}" if p and p["end"] > p["start"] else \
            f"{e['path']}:{e['line']}" if e["line"] else e["path"]
        L.append(f"  E{e['id']} {loc} [{e['extractor']}{'; ' + meta if meta else ''}]"
                 + (f" under '{p['heading']}'" if p and p.get("heading") else ""))
        body = p["text"] if p else e["quote"]
        L += [f"      | {ln}" for ln in body.splitlines()[:12]]
    t = ctx["truncation"]
    if t["truncated"]:
        more = ", ".join(f"{n} more {k}" for k, n in t["omitted_counts"].items())
        L.append(f"OMITTED: {more or 'nothing'}; passages {t['passages']}. Narrow with focus=..., or call "
                 "component/conflicts/risks/decisions for full lists.")
    return "\n".join(L)


def context(db, name: str, focus: str | None = None, fmt: str = "text", max_chars: int = 11000, max_items: int = 8):
    """Print the context package within max_chars: passages are dropped first, then lists are shortened."""
    render = (lambda c: json.dumps(c, indent=1, ensure_ascii=False)) if fmt == "json" else render_context
    passages = True
    while True:
        ctx = component_context(db, name, focus=focus, max_items=max_items, passages=passages)
        out = render(ctx)
        if len(out) <= max_chars or ("component" not in ctx):
            break
        if passages:
            passages = False
        elif max_items > 1:
            max_items = max(1, max_items // 2)
        else:
            break  # smallest package; still over budget, but never cut mid-structure
    print(out)


def run_tool(db, ro, name: str, args: dict) -> str:
    """Run one read-only tool. Shared by ask.py and the MCP server."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            if name == "search":
                search(db, args["text"])
            elif name == "context":
                context(db, args["name"], focus=args.get("focus") or None, fmt=args.get("format") or "text",
                        max_chars=int(args.get("max_chars") or 11000))
            elif name in {"component", "impact"}:
                globals()[name](db, args["name"])
            elif name == "conflicts":
                conflicts(db, args.get("topic") or None)
            elif name == "risks":
                risks(db, args.get("target") or None)
            elif name == "changes":
                changes(db, args.get("run") or None)
            elif name == "decisions":
                decisions(db, args.get("target") or None)
            elif name == "read_source":
                row = db.execute("SELECT text FROM source WHERE path = ?", (args["path"],)).fetchone()
                if not row or row[0] is None:
                    print("no such text file")
                else:
                    lines = row[0].splitlines()
                    a = max(int(args.get("start") or 1), 1)
                    b = min(int(args.get("end") or a + 60), len(lines), a + 200)
                    for i in range(a, b + 1):
                        print(f"{i:>4}: {lines[i - 1]}")
            elif name == "sql":
                cur = ro.execute(args["query"])
                cols = [d[0] for d in cur.description or []]
                print(" | ".join(cols))
                for row in cur.fetchmany(80):
                    print(" | ".join(str(v)[:200] for v in row))
            else:
                print(f"unknown tool: {name}")
        except SystemExit as e:
            print(e.code if isinstance(e.code, str) and e.code else e)
        except (sqlite3.Error, KeyError, ValueError) as e:
            print(f"tool error: {e}")
    out = buf.getvalue()
    return out[:24000] + ("\n...(truncated)" if len(out) > 24000 else "") or "(no results)"


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in {"impact", "component", "conflicts", "risks", "search", "changes",
                                                 "decisions", "context"}:
        sys.exit(__doc__)
    if sys.argv[1] == "context":
        args, focus, fmt = [], None, "text"
        it = iter(sys.argv[2:])
        for a in it:
            if a == "--json":
                fmt = "json"
            elif a == "--focus":
                focus = next(it, None)
            elif a.startswith("--focus="):
                focus = a.split("=", 1)[1]
            else:
                args.append(a)
        if not args:
            sys.exit("usage: query.py context <name> [--focus WORD] [--json]")
        context(connect(), " ".join(args), focus=focus, fmt=fmt, max_chars=10**9)
        return
    cmd, arg = sys.argv[1], " ".join(sys.argv[2:]) or None
    if cmd in {"impact", "component", "search"} and not arg:
        sys.exit(f"usage: query.py {cmd} <name>")
    globals()[cmd](connect(), arg)


if __name__ == "__main__":
    main()
