# Context Service: evidence-backed engineering context for AI coding assistants

> AT&T Hackathon 2026: *Efficient AI Context Service for Enterprise Engineering Knowledge*

Engineering knowledge is scattered: code, specs, wikis, ADRs, meeting notes, incidents,
diagrams and deployment files, and those sources often contradict each other. An AI coding
assistant that doesn't know this writes code that is inconsistent, insecure or built on stale docs.

This project is an **automated pipeline** that reads such a repository (read-only) and builds
one knowledge base out of it. It then serves that knowledge base to the **AI agent in your IDE
over MCP**. Every fact the agent gets carries a **source (`file:line`) and a confidence**.

Inside Cursor or Claude Code, the agent can find out:

- **what breaks if it changes a component:** dependents across code, configs and deployment,
  the owner, the data, and the docs that mention it
- **which docs and diagrams it must not trust:** stale wiki pages, removed endpoints and columns,
  outdated diagrams, and which source is current
- **which decisions and policies are in force:** accepted ADRs vs mere proposals
- **which security patterns it must not repeat:** e.g. a missing ownership check, identity
  taken from a client header, SQL built by interpolation, each with "do instead" guidance
- **what changed since the last ingest:** new and resolved findings

---

## Setup

**Requirements:**
- Python **3.10+**
- `git`
- **PyYAML** ≥ 6, used by the ingest
- The **MCP Python SDK** (`mcp` ≥ 2.3), used by the server
- An **OpenRouter API key** for the AI stages. It's optional: without one, the deterministic stage still runs.

```sh
# 1. dependencies
pip install "pyyaml>=6"
python3 -m pip install --target mcp/.vendor -r mcp/requirements.txt   # MCP SDK, for this exact python3

# 2. API key (git-ignored file, or export it in your shell)
echo "OPENROUTER_API_KEY=sk-or-..." > .env

# 3. build the knowledge base (reads https://github.com/arkik7/att-hackathon-task, never modifies it)
python3 ingest.py
```

Re-running `ingest.py` after new commits updates `context.db` in place: only files whose Git blob
changed are re-extracted, cross-source checks are recomputed, and an unchanged snapshot is a
no-op. `--no-llm` builds the rules-only stage; a later normal run adds only the missing AI work.
See [Keeping context.db up to date](WORKFLOW.md#keeping-contextdb-up-to-date).

On NixOS:
- run the ingest with `nix-shell -p python3 python3Packages.pyyaml git --run 'python3 ingest.py'`
- install the SDK with a pip of the same Python version, e.g.
  `nix-shell -p python314Packages.pip --run 'python3 -m pip install --target mcp/.vendor -r mcp/requirements.txt'`

If the SDK is missing or was built for another Python, the server prints the exact command to fix it.

### Connect your IDE

The repository already includes the MCP configuration. Open the project folder and enable the
`context-service` server:

| IDE / agent | Config file | Notes |
|---|---|---|
| **Cursor** | `.cursor/mcp.json` | Settings → MCP → enable *context-service* |
| **Claude Code** | `.mcp.json` | approve the project server when asked; check with `/mcp` |
| other MCP clients | `python3 mcp/server.py` (stdio) | or `python3 mcp/server.py --http` → `http://127.0.0.1:8765/mcp` |

The server is **read-only** and opens `context.db` on every call, so it can stay running while you re-ingest.

### Try it in the IDE

Ask the agent things like:

```
Before you add a "get order by id" endpoint to order-service, check the context service
for risks and decisions that apply, then implement it.

Do the files in docs/ still apply to the code? Which ones should I not trust?

If I change the user-service API, what else breaks and who owns it?

What got worse since the last ingest?
```

The agent calls the tools itself. Example of what it receives from `risks("order-service")`:

```
[HIGH 0.65] code-review-llm: GET /v1/orders/:id loads the order purely by primary key and never compares
order.customer_id with the caller's identity ... (IDOR / broken object-level authorization).
    do instead: After loading the order, verify order.customer_id === identity.customerId and return 404 ...
    <- apps/order-service/src/index.ts:41  "const identity = readDemoIdentity(req.get(demoIdentityHeader));"
```

### MCP tools

| Tool | Returns |
|---|---|
| `status` | whether `context.db` exists, its size, the last ingest run |
| `impact(name)` | owner, data, direct and transitive dependents, then the top decisions, conflicts and risks for a component |
| `component(name)` | every relation of one component, with evidence |
| `conflicts(topic)` | contradictions, gaps and stale docs, with the verifier's verdict |
| `risks(target)` | security findings plus the guidance generated code must follow |
| `decisions(target)` | ADRs, policies and constraints, and whether each is in force, proposed or superseded |
| `changes(run)` | ingest history, and the findings new or resolved since the previous run |
| `search(text)` | full-text search over every ingested file |
| `read_source(path, start, end)` | lines of a repository file |
| `sql(statement)` | read-only SQL over the knowledge base |

Outputs are kept compact for the agent's context window. If a result is cut, it says how to narrow the request.

---

## How it works

```
any git repo ──► collect ─► classify ─► extract (parsers) ─► AI code pass ─► cross-check ─► security scan
                                                                                               │
          context.db ◄── verify (AI) ◄── diagrams (vision AI) ◄── prose → facts, decisions (AI) ◄┘
              │
              └──► MCP server (mcp/server.py) ──► the AI agent in your IDE
```

1. **Collect and classify.** Every file is typed by standard names, extensions and content
   (`openapi:` → API spec, `apiVersion:` + `kind:` → Kubernetes). **No folder-name rules**, so
   it works on any repo layout. Prose gets its real type (wiki, ADR, incident, …) from the AI,
   and generated code is skipped.
2. **Parse the standard formats.** Components are discovered from manifests
   (npm, Python, Go, Maven, Gradle, Cargo, .NET, …). The pipeline then reads:
   - compose and Kubernetes dependencies
   - OpenAPI endpoints
   - CODEOWNERS
   - routes and calls in common frameworks
   - the **real DB schema**, by replaying the migrations in an in-memory SQLite
3. **AI code pass.** Reads routes, outbound calls, SQL, events and security problems in any language.
4. **Cross-check** facts against each other: spec vs code, calls to missing routes, docs
   describing removed endpoints or columns, missing deployment dependencies, missing owners.
5. **Read prose and diagrams.** The AI turns docs into claims and **decisions/policies**, and
   diagrams into boxes and arrows.
6. **Verify.** For each component, the AI compares code facts with dated doc claims and with the
   decisions that govern it. It confirms or rejects candidates, finds new contradictions, and
   says which source is current.

The knowledge model is defined in **[ONTOLOGY.md](ONTOLOGY.md)**: entity and relation types,
statuses, confidence scale and validation gates. **[WORKFLOW.md](WORKFLOW.md)** walks through
every step, the design decisions and the limits.

### Why the answers can be trusted

- **Every fact has evidence:** file, line, exact quote and the extractor that produced it.
- **Anti-hallucination gate:** an AI-extracted item is kept only if its quote really occurs in the file.
- **Closed vocabulary:** only the entity and relation types defined in the ontology are accepted.
- **Trust order:** code and config (`observed`) beat specs and docs (`declared`), which beat
  archive and history (`outdated`). AI facts are capped below parser facts.
- **No evidence, no link.** A call whose target can't be resolved uniquely stays unresolved
  instead of being guessed. The same API name in two projects is not a relationship.
- **Repository text is data, never instructions.** The repo contains AI-agent instruction
  files, and they are flagged as a prompt-injection risk.

### Tested beyond the target repo

The same code, unchanged, was run on
[GoogleCloudPlatform/microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo).
That repo uses five languages, gRPC instead of REST, and Kubernetes instead of compose. The
pipeline found all 11 services, the real call graph, owners, diagrams and security risks.

---

## Command line

```sh
python3 ingest.py                                  # default: arkik7/att-hackathon-task
python3 ingest.py --repo-url <any git url> --db other.db
python3 ingest.py --no-llm                         # deterministic stage only (free)
python3 ingest.py --show-unresolved                # call targets that could not be attributed

python3 query.py impact user-service               # the same queries the MCP tools run, no AI
python3 query.py decisions order-service
python3 query.py changes
```

`ask.py "question"` is a **fallback** for machines without an MCP-capable IDE. It is a small
agent with the same tools, using `REASON_MODEL`.

### Configuration

| Variable | Purpose |
|---|---|
| `OPENROUTER_API_KEY` | Enables the AI stages. Read from the environment or a local `.env` |
| `EXTRACT_MODEL` | Model for per-file extraction (default in `llm.py`) |
| `REASON_MODEL` | Model for diagrams, verification and `ask.py` (default in `llm.py`) |
| `CONTEXT_DB` | Database for `query.py`, `ask.py` and the MCP server (default `context.db`) |

The knowledge base is a single SQLite file. You can browse it with
[DB Browser for SQLite](https://sqlitebrowser.org/): `sqlitebrowser context.db`.

| Table | Contents |
|---|---|
| `source` | every file: type, dates, status, summary, text (+ full-text index) |
| `component` | services, libraries, endpoints, tables (with live columns), teams, decisions/policies/constraints |
| `relation` | `src TYPE dst` with status (observed / declared / outdated) and confidence |
| `evidence` | file, line, quote and extractor behind every fact |
| `claim` | what each document asserts about a component |
| `conflict` | conflicts / gaps / stale docs, with verifier verdict and rationale |
| `risk` | security concern, severity, guidance for generated code |
| `run`, `change` | ingest history and the new / resolved findings per run |
| `stage_state`, `extraction_unit` | source commit, blob ids, signatures and stage completion (provenance) |

The full model is in [ONTOLOGY.md](ONTOLOGY.md).

---

## Project structure

| File | Role |
|---|---|
| `ingest.py` | The pipeline: collect, classify, parse, cross-check, security scan, run diff |
| `enrich.py` | AI stages: code pass, prose (facts, claims, decisions), diagrams, verification |
| `llm.py` | OpenRouter client with retries and an on-disk response cache |
| `query.py` | The read-only queries shared by the MCP server, the CLI and `ask.py` |
| `mcp/server.py` | MCP server exposing the queries to IDE agents (stdio or HTTP; `.cursor/mcp.json`, `.mcp.json`) |
| `gitsnap.py` | Read-only Git access: identity, blob ids, diffs, clean-checkout check |
| `ask.py` | Fallback CLI agent over the same tools, with citation checking |
| `tests/` | Incremental-ingest tests on temporary Git repositories (mocked model, no API calls) |
| `ONTOLOGY.md` | Entity and relation types, statuses, confidence, validation gates |
| `WORKFLOW.md` | Detailed walkthrough of every step, design decisions and limits |

## Limits and next steps

- **Limits:**
  - Migration replay only understands SQLite-compatible SQL.
  - gRPC `.proto` files are indexed but not yet parsed into endpoints.
  - Calls through variables with no address evidence stay unresolved (by design).
  - Incremental ingest re-runs more than strictly needed when shared context changes (see WORKFLOW).
  - AI findings vary somewhat with the model chosen. Verdicts from a stronger `REASON_MODEL` are sharper.
- **Next steps:**
  - Run `ingest.py` from CI or a post-merge hook, so the database follows the default branch.
  - Multiple repositories in one knowledge base, for cross-repo relationships.
  - A "before you edit" MCP tool that bundles impact, risks and decisions for a file.
