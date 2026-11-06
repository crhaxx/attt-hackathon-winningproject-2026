#!/usr/bin/env python3
"""Query the Context Service database built by ingest.py.

Every answer line is: claim  [confidence]  <- source:line "quote"

    python3 query.py impact order-service     # what breaks if I change it
    python3 query.py component user-service   # everything known about it
    python3 query.py conflicts [topic]         # contradictions, gaps, stale docs
    python3 query.py risks [path-or-component] # security concerns + guidance
    python3 query.py search "minor units"      # full-text over all sources
    python3 query.py changes [run-id]          # new / resolved findings since the previous ingest
    python3 ask.py "free-form question"         # LLM agent over these tools (OpenRouter)
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import sys
from pathlib import Path

DB = Path(os.environ.get("CONTEXT_DB") or Path(__file__).resolve().parent / "context.db")
# edges along which a change propagates from dst back to src
DEPENDENCY_EDGES = ("DEPENDS_ON", "CALLS", "CONFIGURED_TO_CALL", "DEPLOY_DEPENDS_ON")


def connect() -> sqlite3.Connection:
    if not DB.exists():
        sys.exit("context.db not found - run: python3 ingest.py")
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    return db


def resolve(db, name: str) -> str:
    row = db.execute(
        "SELECT id FROM component WHERE id = ? OR name = ? OR id LIKE ? "
        "ORDER BY CASE WHEN id LIKE 'comp:%' THEN 0 ELSE 1 END, length(id) LIMIT 1",
        (name, name, f"%:{name}"),
    ).fetchone()
    if not row:
        sys.exit(f"unknown component: {name}")
    return row["id"]


def cite(db, ev_id: int) -> str:
    e = db.execute("SELECT path, line, quote FROM evidence WHERE id = ?", (ev_id,)).fetchone()
    loc = f"{e['path']}:{e['line']}" if e["line"] else e["path"]
    return f'{loc}  "{e["quote"][:90]}"'


def impact(db, name: str):
    root = resolve(db, name)
    print(f"Changing {root} can affect:\n")
    seen, frontier, depth = {root}, [root], 0
    marks = ",".join("?" * len(DEPENDENCY_EDGES))
    while frontier:
        depth += 1
        nxt = []
        for node in frontier:
            for r in db.execute(
                f"SELECT * FROM relation WHERE dst = ? AND type IN ({marks}) AND status != 'outdated' "
                "ORDER BY confidence DESC", (node, *DEPENDENCY_EDGES)):
                extra = json.loads(r["props"] or "{}")
                via = f" {extra.get('method', '')} {extra.get('path', '')}".rstrip() if "path" in extra else ""
                hop = "directly" if depth == 1 else f"transitively (hop {depth}, via {node})"
                print(f"  {r['src']:<28} {r['type']}{via} {hop}  [{r['confidence']}]\n      <- {cite(db, r['evidence_id'])}")
                if r["src"] not in seen:
                    seen.add(r["src"])
                    nxt.append(r["src"])
        frontier = nxt
    tables = db.execute("SELECT dst FROM relation WHERE src = ? AND type = 'HAS_TABLE'", (root,)).fetchall()
    if tables:
        print("\n  Owns data:", ", ".join(t["dst"] for t in tables))
    owners = db.execute("SELECT DISTINCT dst FROM relation WHERE src = ? AND type = 'OWNED_BY'", (root,)).fetchall()
    print("  Owner:", ", ".join(o["dst"] for o in owners) or "NONE (gap)")
    docs = db.execute("SELECT r.dst, r.props FROM relation r WHERE src = ? AND type = 'MENTIONED_IN'", (root,)).fetchall()
    if docs:
        print(f"  Docs to update ({len(docs)}):", ", ".join(d["dst"].removeprefix("source:") for d in docs))
    short = root.split(":", 1)[-1]
    conflicts(db, short, header="\n  Known conflicts touching it:")
    risks(db, short, header="\n  Security concerns to not repeat:")


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


def conflicts(db, topic: str | None = None, header: str | None = None):
    q = "SELECT * FROM conflict WHERE IFNULL(verdict, '') != 'rejected'"
    args = ()
    if topic:
        q += " AND (subject LIKE ? OR description LIKE ?)"
        args = (f"%{topic}%", f"%{topic}%")
    rows = db.execute(q + " ORDER BY confidence DESC", args).fetchall()
    if header and rows:
        print(header)
    for c in rows:
        tag = f" {c['verdict']}" if c["verdict"] else ""
        print(f"  [{c['kind'].upper()} {c['confidence']}{tag}] {c['description']}")
        if c["rationale"]:
            print(f"      verifier: {c['rationale']}")
        for ev in json.loads(c["evidence_ids"]):
            print(f"      <- {cite(db, ev)}")
    if not rows and not header:
        print("no conflicts found")


def risks(db, target: str | None = None, header: str | None = None):
    q = "SELECT * FROM risk WHERE rule != 'agent-instructions-in-corpus'"
    args = ()
    if target:
        q += " AND (subject LIKE ? OR evidence_id IN (SELECT id FROM evidence WHERE path LIKE ?))"
        args = (f"%{target}%", f"%{target}%")
    rows = db.execute(q + " ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC",
                      args).fetchall()
    if header and rows:
        print(header)
    for r in rows:
        print(f"  [{r['severity'].upper()} {r['confidence']:.2f}] {r['rule']}: {r['description']}\n"
              f"      do instead: {r['guidance']}\n      <- {cite(db, r['evidence_id'])}")
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
    for c in rows:
        sign = "+" if c["change"] == "new" else "-"
        print(f"  {sign} {c['change'].upper()} {c['item']}: {c['summary']}")
        if latest and c["ref_id"]:
            table = "conflict" if c["item"] == "conflict" else "risk"
            row = db.execute(f"SELECT * FROM {table} WHERE id = ?", (c["ref_id"],)).fetchone()
            evs = json.loads(row["evidence_ids"]) if table == "conflict" else [row["evidence_id"]]
            for ev in evs[:3]:
                print(f"      <- {cite(db, ev)}")


def run_tool(db, ro, name: str, args: dict) -> str:
    """Run one read-only tool. Shared by ask.py and the MCP server."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            if name == "search":
                search(db, args["text"])
            elif name in {"component", "impact"}:
                globals()[name](db, args["name"])
            elif name == "conflicts":
                conflicts(db, args.get("topic") or None)
            elif name == "risks":
                risks(db, args.get("target") or None)
            elif name == "changes":
                changes(db, args.get("run") or None)
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
    if len(sys.argv) < 2 or sys.argv[1] not in {"impact", "component", "conflicts", "risks", "search", "changes"}:
        sys.exit(__doc__)
    cmd, arg = sys.argv[1], " ".join(sys.argv[2:]) or None
    if cmd in {"impact", "component", "search"} and not arg:
        sys.exit(f"usage: query.py {cmd} <name>")
    globals()[cmd](connect(), arg)


if __name__ == "__main__":
    main()
