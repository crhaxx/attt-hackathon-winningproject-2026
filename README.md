# Read-Only

**Evidence-backed engineering context for AI coding assistants.**<br>
AT&T Hackathon 2026: *Efficient AI Context Service for Enterprise Engineering Knowledge*

Engineering knowledge is scattered across code, specs, wikis, ADRs, meeting notes, incidents,
diagrams and deployment files, and those sources often contradict each other. This project reads
such a repository, builds one knowledge base from it, and serves it to **the AI agent in your
IDE over MCP**. Every answer comes with its **source (`file:line`) and a confidence**.

The agent can ask:

- **What breaks if I change this?** Dependents, the owner, data, related docs
- **Which docs can't I trust?** Stale pages, removed endpoints, outdated diagrams
- **Which decisions are in force?** Accepted ADRs and policies vs mere proposals
- **Which security mistakes must I avoid here?** Each comes with "do instead" guidance
- **What changed since the last ingest?**

> New to this? [EXPLAIN.md](EXPLAIN.md) walks through the whole system in plain language.

## Quick start

```sh
pip install "pyyaml>=6"
python3 -m pip install --target mcp/.vendor -r mcp/requirements.txt   # MCP SDK (≥ 2.3)
echo "OPENROUTER_API_KEY=sk-or-..." > .env                            # optional, enables the AI stages
python3 ingest.py                                                     # builds context.db
```

Needs Python 3.10+ and git. The source repo
([arkik7/att-hackathon-task](https://github.com/arkik7/att-hackathon-task)) is only read, never
modified. Re-running `ingest.py` only re-processes files that changed.

<details>
<summary>NixOS</summary>

```sh
nix-shell -p python3 python3Packages.pyyaml git --run 'python3 ingest.py'
nix-shell -p python314Packages.pip --run 'python3 -m pip install --target mcp/.vendor -r mcp/requirements.txt'
```
Use the pip that matches your Python version. If the SDK doesn't fit, the server prints the exact fix.
</details>

## Use it from your IDE

The MCP config is included. Open the project and enable the **read-only** server:

- **Cursor:** `.cursor/mcp.json` (Settings → MCP)
- **Claude Code:** `.mcp.json` (approve it, check with `/mcp`)
- **Anything else:** `python3 mcp/server.py` (stdio) or `--http` (`http://127.0.0.1:8765/mcp`)

Then just ask:

```
Before adding a "get order by id" endpoint to order-service, check the risks and decisions that apply.
Which files in docs/ should I not trust?
If I change the user-service API, what breaks and who owns it?
```

What the agent gets back, for example from `risks("order-service")`:

```
[HIGH 0.65] GET /v1/orders/:id never compares order.customer_id with the caller's identity (IDOR).
    do instead: verify order.customer_id === identity.customerId and return 404 otherwise.
    <- apps/order-service/src/index.ts:44  .prepare("SELECT * FROM orders WHERE id = ?")
```

**Tools:** `context` (start here: everything about one component, in one call) · `impact` · `component` · `conflicts` · `risks` · `decisions` · `changes` · `search` · `read_source` · `sql` · `status`

## How it works

```
repo ─► classify ─► parse standard formats ─► AI reads code ─► cross-check ─► security scan
                                                                                   │
  IDE agent ◄── MCP ◄── context.db ◄── AI verifies ◄── AI reads docs & diagrams ◄──┘
```

1. **Parsers** read what's certain: manifests, compose/Kubernetes, OpenAPI, CODEOWNERS, and the
   real DB schema, by replaying the migrations.
2. **AI** reads what needs understanding: code in any language, prose, decisions, diagrams.
   Decisions about who may call whom are then checked against the real calls, without AI: each call gets
   *satisfied*, *violated*, *permitted by exception*, *unknown*, *not applicable* or *conflicting rules*.
3. **Cross-checks** find contradictions, gaps and stale docs. An **AI verifier** confirms or rejects them.

**Why you can trust it:**
- **Receipts:** every fact stores its file, line and quote.
- **Quote check:** AI output is dropped if its quote isn't really in the file.
- **Code beats docs:** what runs (`observed`) outranks what's written (`declared`) and what's archived (`outdated`).
- **No guessing:** unclear links are left out.
- **No taking orders:** repo text is treated as data, never as instructions.

It isn't tied to one repo: it was tested unchanged on
[Google's microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo)
(five languages, gRPC, Kubernetes).

## Docs

| | |
|---|---|
| [EXPLAIN.md](EXPLAIN.md) | the whole system in plain language |
| [WORKFLOW.md](WORKFLOW.md) | every step in detail, design decisions, incremental ingest |
| [ONTOLOGY.md](ONTOLOGY.md) | the knowledge model: entities, relations, statuses, confidence |

<details>
<summary>Command line, configuration and files</summary>

```sh
python3 ingest.py --repo-url <git url> --db other.db   # any other repository
python3 ingest.py --no-llm                             # rules only, free
python3 query.py context inventory-service             # same queries as the MCP tools
python3 query.py impact user-service
python3 ask.py "question"                              # fallback agent without an IDE
python3 -m unittest discover -s tests                  # tests (mocked model, no API calls)
```

| Variable | Purpose |
|---|---|
| `OPENROUTER_API_KEY` | enables the AI stages (env or `.env`) |
| `EXTRACT_MODEL`, `REASON_MODEL` | models for extraction and for verification (defaults in `llm.py`) |
| `CONTEXT_DB` | database used by `query.py`, `ask.py` and the MCP server |

| File | Role |
|---|---|
| `ingest.py` | pipeline: classify, parse, cross-check, security scan, run diff |
| `enrich.py` | AI stages: code, prose, diagrams, verification |
| `query.py` | read-only queries shared by MCP, the CLI and `ask.py` |
| `mcp/server.py` | MCP server for IDE agents |
| `gitsnap.py` | read-only Git access for incremental ingest |
| `policy.py` | checks decisions' call rules against observed calls (six outcomes, no AI) |
| `llm.py` | OpenRouter client with a response cache |

Browse the database with [DB Browser for SQLite](https://sqlitebrowser.org/): `sqlitebrowser context.db`.
</details>

## Limits and next steps

- **Limits:** one repo per database; only SQLite-compatible migrations; `.proto` files aren't parsed
  yet; calls with no address evidence stay unresolved; AI verdicts depend on the model.
- **Next steps:** run the ingest from CI on every merge, put several repos in one knowledge base,
  and add a "before you edit" tool that bundles impact, risks and decisions.
