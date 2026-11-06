# Context Service: how the workflow works

One command, `python3 ingest.py`, turns the engineering repository
[arkik7/att-hackathon-task](https://github.com/arkik7/att-hackathon-task) into one SQLite
knowledge base, `context.db`. The source repository is only cloned and read, never modified.
Questions are then answered from that database. Every answer gives a **claim + source
(file:line) + confidence**.

```
GitHub repo ──clone──► 1 Collect ─► 2 Classify ─► 3 Extract (rules) ─► 4 Cross-check ─► 5 Security scan
                                                                                            │
                     context.db ◄── 8 Verify (Sonnet) ◄── 7 Diagrams (Sonnet) ◄── 6 Prose (Haiku)
                         │
                         ├──► query.py   fixed questions, no AI
                         └──► ask.py     natural-language questions (AI agent)
```

Steps 1–5 are **stage 1**: plain code, no AI, free, and the same result on every run.
Steps 6–8 are **stage 2**: AI models through OpenRouter. `--no-llm` stops after stage 1.

---

## Stage 1: deterministic

### 1. Collect
The repo is cloned into `.cache/source`, or pulled if it is already there. For every file
tracked by git, the pipeline stores its path, size, text and last commit date. It also stores
any date the file states about itself (`2026-03-10-api-review.md`, `Date: 2026-03-11`). Those
dates later decide which of two conflicting sources is newer.

### 2. Classify
Path rules give each file a type: code, test, config, dependency manifest, deployment, CI,
API spec, schema, wiki, ADR, runbook, meeting note, incident, issue, release note, diagram,
fixture, agent instructions. Files under `archive/` or `history/` are marked as archived, so
their facts are stored as `outdated`.

### 3. Extract with rules
Structured files are read by parsers and regex rules (search patterns for text). These are
the most reliable facts, with confidence 0.85–0.95 and status `observed`.

| From | Extracted |
|---|---|
| `package.json`, `requirements.txt` | which package uses which (internal `workspace:` packages vs external libraries with versions) |
| code (Express, FastAPI, Java `HttpServer`) | the routes each service exposes |
| code + `config/ports.json` | service-to-service calls: `users = USER_SERVICE_URL ?? "…:5002"` → port 5002 = user-service → `${users}/v2/users/…` is a call |
| SQL strings in code | which tables a service reads and writes |
| `compose.yaml` | deployment dependencies and the service URLs each container is configured with |
| OpenAPI JSON | endpoints the specs promise (status `declared`, not `observed`) |
| migrations | each service's migrations are **replayed in an in-memory SQLite**. The result is the real final schema and every dropped or renamed column |
| `CODEOWNERS` | which team owns which component |
| docs | which components each doc mentions and which endpoints it names |

Every fact gets an **evidence** row: file, line, exact quote, and the extractor that produced it.

### 4. Cross-check
The extracted facts are compared with each other, producing conflicts, gaps and stale docs:
- the spec promises an endpoint the code doesn't have, or the code has a route no spec documents
- code calls a route the target service doesn't have (contract break)
- a doc describes an endpoint or a column that no longer exists
- code calls another service, but compose doesn't declare the dependency
- port numbers disagree between config files
- a component has no owner

### 5. Security scan
Rules over code and config look for:
- hardcoded secrets
- CORS that allows any origin
- caller identity taken from a header the client controls
- SQL built by string interpolation
- internal error messages returned to callers
- whole records written to logs
- disabled TLS checks

Each finding includes "do instead" guidance, so AI coding assistants don't repeat it. The
`.agents/` files are flagged as a prompt-injection risk.

---

## Stage 2: AI via OpenRouter

Models: `EXTRACT_MODEL` (default `anthropic/claude-haiku-5.5`, cheap) and `REASON_MODEL`
(default `anthropic/claude-sonnet-5.5`). Every answer is cached in `.cache/llm/` under a
hash of the request, so reruns cost nothing and give the same result.

### 6. Prose → facts (Haiku)
A *prose file* is written in human language rather than code: wiki pages, ADRs, meeting
notes, incidents, issues, release notes, READMEs. Each one is read in parallel. Given the
list of known components, the model returns JSON with:
- the document's status: current, historical, proposal or superseded
- facts about relationships between components
- concrete claims, e.g. "orders expose `total` in dollars"
- risks the document mentions

Two safeguards:
- **Quote check:** an item is kept only if its quote really occurs in the file.
  Anything else is dropped as made up.
- **Confidence cap:** prose facts top out at 0.7, so a document never outranks code.

### 7. Diagrams (Sonnet, vision)
Each PNG becomes a list of boxes and arrows mapped to known components. Plain code then
compares the arrows with the real call graph. It flags arrows that have no matching call,
and calls the diagram leaves out. This is how an outdated architecture diagram is detected.

### 8. Verify (Sonnet)
For each component, the model sees:
- what the code does (observed facts, with evidence ids)
- what the docs claim, with their dates and status
- the rule-based candidates from step 4

The model then:
- **confirms or rejects** each candidate. Rejected ones stay in the database with the
  reason, for human review.
- **reports new contradictions** that rules can't see, and says which source is current
  and why.
- may **cite only the evidence ids it was given**. Anything else is dropped.

---

## Design rules

- **Rules first, AI second.** Structured files get parsers; AI handles only prose.
- **Trust order:** code/config (`observed`) beats specs/docs (`declared`), which beat
  archive/history (`outdated`). Every item also carries a confidence score from 0 to 1.
- **Repository text is data, never instructions.** Every prompt says so, and every AI
  output is validated against the source.
- **No evidence, no link.** The same API name in two projects is not a relationship
  without proof.

## Example: one finding traced end to end

1. `wiki/orders-client.md` is classified as wiki, dated 2026-02-25. ADR 002 (2026-03-11)
   is marked current.
2. Migration replay: `orders` has `total_minor` and no `total`. The drop is evidenced at
   `apps/order-service/migrations/002-minor-units.sql:3`, `ALTER TABLE orders DROP COLUMN total;`.
3. Haiku extracts the claim "order responses expose total as a decimal number of dollars",
   and its quote is found on line 5 of the wiki page.
4. The rule-based candidate `doc-describes-dropped-column` is **confirmed** by Sonnet
   (confidence 0.85).

The same check also flagged `issues/ACC-42.md`. The verifier **rejected** it, because
that issue describes the rename itself, so it isn't stale.

---

## The database (`context.db`)

| table | contents |
|---|---|
| `source` | every file: type, dates, text, status and summary (+ `source_fts` full-text index) |
| `component` | apps, packages, endpoints, tables (with live columns), teams, libraries |
| `relation` | `src TYPE dst`, with status (observed / declared / outdated) and confidence |
| `evidence` | file, line, quote and extractor behind every relation, claim, conflict and risk |
| `claim` | what each document asserts about a component |
| `conflict` | conflict / gap / stale, with evidence, verifier verdict and rationale |
| `risk` | security concern, severity, guidance for generated code |

Open it with DB Browser for SQLite: `sqlitebrowser context.db`.

---

## What each Python file does

```
ingest.py ──(stage 2)──► enrich.py ──► llm.py ──► OpenRouter
    │
    ▼ writes
context.db ◄── reads ── query.py ◄── uses ── ask.py ──► llm.py
```

### `ingest.py`: builds the knowledge base
The main pipeline. It defines the schema and runs stage 1: clone, classify, the `extract_*`
rules, the `check_*` cross-checks and the security `RULES`. When a key is available it calls
`enrich.py`, then prints a summary.
Options: `--no-llm`, `--repo <checkout>`, `--db <file>`, `--workers <n>`.

### `enrich.py`: the AI part of the ingest
Stage 2, called by `ingest.py`:
- `extract_prose()`: prose → facts/claims/risks, with the `locate_quote()` check
- `read_diagrams()`: diagrams → nodes and edges, compared with the call graph
- `verify()`: confirms or rejects candidates and reports new contradictions

### `llm.py`: connection to OpenRouter
Loads `OPENROUTER_API_KEY` from the environment or `.env` and sets the two models.
`chat()` handles retries and the response cache. `chat_json()` parses JSON answers.
`image_part()` packs a PNG for the vision model.

### `query.py`: fixed questions, no AI
```sh
python3 query.py impact user-service     # what breaks if it changes (transitive), owner, data, docs, conflicts, risks
python3 query.py component api-gateway   # everything connected to one component
python3 query.py conflicts total         # contradictions / gaps / stale docs with verdicts
python3 query.py risks order-service     # security findings + "do instead"
python3 query.py search "minor units"    # full-text search over every file
```
Fast, free and deterministic. A safe fallback for demos.

### `ask.py`: questions in plain language
```sh
python3 ask.py "If I change how order-service stores money, what breaks and which docs are wrong?"
python3 ask.py --show-tools "Where is the auth model weak?"
```
A Sonnet agent with tools: the five `query.py` functions, `read_source` and read-only `sql`.
It answers as claim, `path:line`, confidence, with a *Watch out* section. Afterwards,
`check_citations()` flags any cited file that is not in the repository.

### Generated files (git-ignored)
- `context.db`: the knowledge base, rebuilt on every ingest.
- `.cache/source/`: the read-only clone of the source repo.
- `.cache/llm/`: cached AI responses.
- `.env`: the OpenRouter key.

---

## Limits and next steps

- **Limits:**
  - Route and call extraction uses regexes tuned to this repo's frameworks.
  - Diagram mapping depends on the vision model.
  - Claims are only verified if the prose step attached them to a component.
- **Next steps:**
  - An MCP server so IDEs (Claude Code, Cursor) can call `ask`, `impact` and the other tools directly.
  - Incremental re-ingest on each commit (the response cache already makes this cheap).
