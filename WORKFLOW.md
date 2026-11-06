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
When AI is enabled, an extra **AI code pass** (step 3b) runs between extraction and cross-checks.

**Core rule: nothing depends on one repository's layout.** The data can change. The only
assumptions are standard file formats (manifests, OpenAPI, compose, Kubernetes, CODEOWNERS, SQL)
and that the content is engineering knowledge. Folder names like `apps/`, `wiki/` or
`meeting-notes/` are never used to decide anything.

```sh
python3 ingest.py                                   # default repo
python3 ingest.py --repo-url <any git url> --db other.db
python3 ingest.py --repo /path/to/checkout          # local checkout, read-only
python3 ingest.py --no-llm                          # deterministic stage only
python3 ingest.py --show-unresolved                 # list call targets nobody matched
```

Requirements: Python 3.10+, git and PyYAML (`pip install "pyyaml>=6"`, see README; on NixOS
`nix-shell -p python3 python3Packages.pyyaml git`).

---

## Stage 1: deterministic

### 1. Collect
Any git repository is cloned into `.cache/sources/<repo>`, or pulled if it is already there.
For every file tracked by git, the pipeline stores its path, size, text and last commit date.
It also stores any date the file states about itself (`2026-03-10-api-review.md`,
`Date: 2026-03-11`). Those dates later decide which of two conflicting sources is newer.

### 2. Classify, using standard names, extensions and content
- **By standard file name:** `package.json`, `go.mod`, `pom.xml` and `Cargo.toml` are
  dependency manifests. `Dockerfile` and `compose.yaml` are deployment files. `CODEOWNERS`
  defines ownership. `.github/workflows` holds CI.
- **By content:** a YAML or JSON file containing `openapi:` / `swagger:` is an API spec, and one
  with `apiVersion:` + `kind:` is a Kubernetes manifest. Files marked `Code generated … DO NOT EDIT`
  / `@generated`, or named like protoc output (`_pb2.py`, `.pb.go`), are **generated** and skipped.
- **Prose** (`.md`, `.rst`, `.txt`, …) starts as `doc`. The AI later sets its real type from the
  content: wiki, ADR, meeting note, incident, issue, release note, runbook, design doc, readme, ….
- **Archived:** any folder segment like `archive`, `legacy`, `deprecated` or `history` marks the
  file archived, so its facts are stored as `outdated`.

### 3. Discover components and extract from standard formats
**Components** are found from manifests wherever they sit in the tree: `package.json`,
`pyproject.toml`, `go.mod`, `pom.xml`, `build.gradle`, `Cargo.toml`, `*.csproj`, `Gemfile`, ….
- A root manifest in a multi-project repo is treated as a workspace, not a component.
- Test projects are folded into the component they test.
- A manifest in a generic folder (`src/`, `app/`, `server/`) names its parent folder.
- Each component gets **aliases** (manifest name, folder name, compose/Kubernetes service
  name). A name claimed by two components is marked ambiguous and never used to link them.

After the AI code pass a component's kind is set: **service** if it serves routes or listens on
a port, **library** if others depend on it, otherwise **project**.

| From | Extracted |
|---|---|
| manifests (npm, pip/pyproject, Go, Maven, Gradle, Cargo, Gem, Composer) | internal dependencies between components vs external libraries with versions |
| where a component listens | `PORT=` in env files, `EXPOSE` in Dockerfiles, compose/Kubernetes ports, any JSON "port registry" whose keys are component names |
| compose + Kubernetes (real YAML parser) | deployment dependencies, plus the service addresses each container is configured with |
| code, fast path | routes from common frameworks (Express/Koa, Flask/FastAPI, Spring, Go net/http/gin/chi, ASP.NET); calls via env-configured addresses (`USER_SERVICE_URL`, `os.getenv`, `System.getenv`, …) and URL literals; SQL in strings |
| OpenAPI / Swagger (JSON or YAML) | endpoints the specs promise (status `declared`, not `observed`) |
| SQL migrations | **replayed in an in-memory SQLite**. The result is the real final schema and every dropped or renamed column |
| `CODEOWNERS` | owners with git's "last matching rule wins" semantics, and rules that match no file |
| docs | which components each doc mentions and which endpoints it names |

**Resolving "who is this address?"** goes from strongest to weakest evidence:
1. explicit env var names and URLs
2. host names matching a component alias
3. a port that only one component listens on
4. separator- and case-insensitive name matching (`PRODUCT_CATALOG_SERVICE_ADDR` ==
   `productCatalogSvcAddr` == `productcatalogservice`)

If nothing matches uniquely, the call stays **unresolved** and nothing is guessed.

### 3b. AI code pass (Haiku, any language)
Every hand-written code file is read by the AI. It returns:
- routes the file serves, including hand-written dispatch the patterns can't see
- outbound calls, naming the target the way the code does
- SQL tables, events published/consumed, and concrete security problems

Each item is kept only if its quote is really in the file. Calls go through the same resolver.
These facts carry confidence 0.7–0.75, below the deterministic parsers.

Every fact gets an **evidence** row: file, line, exact quote, and the extractor that produced it.

### 4. Cross-check
The extracted facts are compared with each other, producing conflicts, gaps and stale docs:
- the spec promises an endpoint the code doesn't have, or the code has a route no spec documents
- code calls a route the target service doesn't have (contract break)
- a doc describes an endpoint or a column that no longer exists
- code calls another service, but the deployment config doesn't declare the dependency
- a component has no owner, or a CODEOWNERS rule matches no file

### 5. Security scan
Rules over code and config look for:
- hardcoded secrets
- CORS that allows any origin
- caller identity taken from a header the client controls
- SQL built by string interpolation
- internal error messages returned to callers
- whole records written to logs
- disabled TLS checks

Each finding includes "do instead" guidance, so AI coding assistants don't repeat it.
Comment lines (licence headers, commented-out examples) are ignored. Agent instruction files
(`AGENTS.md`, `CLAUDE.md`, `.agents/`, `.cursor/`, …) are flagged as a prompt-injection risk.
Context-specific problems, like a missing ownership check, come from the AI code pass.

---

## Stage 2: AI via OpenRouter

Models: `EXTRACT_MODEL` (the cheap extraction model) and `REASON_MODEL` (diagrams,
verification, Q&A). Both can be set as environment variables; the defaults live in `llm.py`
(currently `deepseek/deepseek-v4.1-flash` for both). The results described in this document
were produced with `anthropic/claude-haiku-5.5` (extract) and `anthropic/claude-sonnet-5.5`
(reason), which is why the steps below name Haiku and Sonnet. Every answer is cached in `.cache/llm/` under a
hash of the request, so reruns cost nothing and give the same result.

### 6. Prose → facts (Haiku)
A *prose file* is written in human language rather than code: wiki pages, ADRs, meeting
notes, incidents, issues, release notes, READMEs. Each one is read in parallel. Given the
list of known components, the model returns JSON with:
- the document's type (wiki, ADR, incident, …), decided from the content and not the folder
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

## Run-to-run diff: what changed since the last ingest

Every run ends with a report of which conflicts and risks are **new** and which are
**resolved** compared with the previous run:

```
run #3 at 6b7bf93, compared with run #2 at 612cc0b (2026-11-06T11:43:00):
  NEW       0 conflicts, 1 risks
    + risk: [high] hardcoded-secret (order-service): Credential-like literal committed to the repository
  RESOLVED  1 conflicts, 0 risks
    - conflict: [stale] doc-describes-dropped-column: wiki/orders-client.md ... describes field 'total' ...
```

**How it works:**
- **Fingerprints.** Each conflict and risk gets a *fingerprint*: a hash of its type, its
  subject, and the file + quote of its evidence. Line numbers are deliberately left out, so
  inserting lines above a finding does not make it look new.
- **Comparison.** Before the database is rebuilt, the old run's active fingerprints and run
  history are read. After the run they are compared: fingerprints that appeared are *new*,
  and ones that disappeared are *resolved*. Conflicts the verifier rejected don't count as active.
- **Like with like.** Runs are compared only if they cover the same repository in the same mode
  (rules-only vs. AI). Otherwise every AI finding would look "resolved" after a rules-only run.
  A run with no comparable predecessor becomes the new baseline.
- **History.** Run history is kept across rebuilds in the `run` and `change` tables.

**Usage:**
```sh
python3 ingest.py                        # prints the diff at the end
python3 query.py changes [run-id]        # run history + what changed, with evidence
python3 ingest.py --repo <checkout> --label <name>   # compare different checkouts of one repo
```
`ask.py` has a `changes` tool too ("what got worse since the last ingest?").

**Tested** on a throwaway local copy of the source repo (the real one is never modified):

| Edit | Result |
|---|---|
| none (two identical runs) | 0 new, 0 resolved |
| fixed `total` → `total_minor` in the wiki | the stale-doc conflict is **resolved** |
| added a hardcoded API key | **new** high risk, key masked in the quote |
| inserted lines above existing findings | no change reported |

**Limit:** rule-based findings are fully stable. AI findings are stable while their inputs are
unchanged, because the response cache replays the same answers. When code changes, evidence
row numbers shift and the verifier's prompts change even for components that didn't change.
Its answers may then be worded differently and show up as false new/resolved churn. The
planned fix is **stable evidence ids** (a hash of path + line + quote), which is also the core of
incremental ingest:
- redo per-file extraction only for changed files
- recompute the global assembly every time (cheap)
- re-verify only the components whose inputs changed

## Design rules

- **No repository-specific rules.** Standard formats get parsers. Everything else (any
  programming language, any doc layout) goes to the AI and is checked against the source.
- **Rules first, AI second.** Parser facts outrank AI facts.
- **No evidence, no guess.** A call target that can't be resolved uniquely stays unresolved
  (`--show-unresolved` lists them). Generated code is not treated as evidence.
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
| `run` | one row per ingest: commit, mode, counts, baseline run |
| `change` | findings new / resolved in each run compared with its baseline |

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
The main pipeline. It defines the schema and runs the deterministic stage:
- clone and `classify()`
- `discover_components()`
- the `extract_*` parsers
- the address resolver (`Ctx.resolve`)
- the `check_*` cross-checks and the security `RULES`

When a key is available it calls the AI stages in `enrich.py`, then prints a summary.
Options: `--repo-url`, `--repo`, `--db`, `--no-llm`, `--workers`, `--show-unresolved`.

### `enrich.py`: the AI part of the ingest
Called by `ingest.py`:
- `code_pass()`: any code file → routes, calls, SQL, events and risks, quote-checked
- `extract_prose()`: prose → doc type, facts, claims and risks, with the `locate_quote()` check
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
python3 query.py changes                 # new / resolved since the previous ingest
```
Fast, free and deterministic. A safe fallback for demos. Set `CONTEXT_DB=<file>` to query
another database (this also works for `ask.py`).

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
- `.cache/sources/<repo>/`: read-only clones of the source repositories.
- `.cache/llm/`: cached AI responses.
- `.env`: the OpenRouter key.

---

## Generality check

The same code, with no changes, was run on an unrelated repository,
[GoogleCloudPlatform/microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo).
That repo uses Go, C#, Node, Python and Java, gRPC instead of REST, and Kubernetes instead of
compose. The run found:
- all 11 services
- the real call graph, from Kubernetes env vars and code
- owners from CODEOWNERS
- 21 diagrams
- security risks such as SQL built by interpolation and plaintext gRPC

The resulting database answered `ask.py` questions with sources. The second repo exposed
generic weaknesses, and all of them were fixed in the general code, not with repo-specific rules:
- name matching that depended on separators
- licence-header noise
- generated gRPC stubs read as real calls

## Limits and next steps

- **Limits:**
  - Migration replay only understands SQLite-compatible SQL. Other dialects are skipped with a note.
  - Calls through variables with no address evidence stay unresolved. This is by design, but it
    means some edges are missing.
  - gRPC contracts (`.proto`) are stored and searchable, but not yet parsed into endpoints.
  - Diagram mapping depends on the vision model.
  - Claims are only verified if the prose step attached them to a component.
- **Next steps:**
  - An MCP server so IDEs (Claude Code, Cursor) can call `ask`, `impact` and the other tools directly.
  - Stable evidence ids and per-file incremental ingest triggered on each commit (see
    *Run-to-run diff* above).
