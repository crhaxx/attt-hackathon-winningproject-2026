# Context Service: how the workflow works

One command, `python3 ingest.py`, turns the engineering repository
[arkik7/att-hackathon-task](https://github.com/arkik7/att-hackathon-task) into one SQLite
knowledge base, `context.db`, and keeps it up to date commit by commit (see
[Keeping context.db up to date](#keeping-contextdb-up-to-date)). The source repository is only
cloned and read, never modified.
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
Any git repository is cloned into `.cache/sources/<repo>`, or pulled if it is already there
(`--repo` uses an existing checkout as is; it must have no uncommitted changes to tracked files).
For every file tracked by git at HEAD, the pipeline stores its path, Git blob id, size, text
(read from the Git object store, not the working tree) and last commit date.
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
- **decisions, policies and constraints**, each with a status (accepted / proposed /
  superseded / rejected), the rule text, and `GOVERNS` links to the components they apply to
  (see [ONTOLOGY.md](ONTOLOGY.md))
- the document's status: current, historical, proposal or superseded
- facts about relationships between components
- concrete claims, e.g. "orders expose `total` in dollars"
- for each fact and claim, its **perspective**: whether the sentence describes the present, a plan,
  an undecided proposal, the past or a reported observation (one page often mixes them)
- **aliases**: other names the document itself ties to a known component ("Edge, the API gateway").
  Kept only with a verbatim quote containing the name, and never when the name already means another
  component. They are used for lookups (`context("Edge")`) and to resolve that same document's facts,
  never by other files' extraction, so results do not depend on processing order.
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
- **Comparison.** The database is updated in place: the active fingerprints are read at the
  start of the update transaction and compared with the result at its end, so the diff and the
  data it describes are published together. Fingerprints that appeared are *new*, and ones that
  disappeared are *resolved*. Conflicts the verifier rejected don't count as active.
- **Like with like.** Runs are compared only if they cover the same repository and the model
  stage is in the same state (complete vs. not, see `stage_state`). Otherwise every AI finding
  would look "resolved" after a rules-only run. A run with no comparable predecessor becomes the
  new baseline.
- **History.** One `run` row per invocation that changed the database (a run with nothing to do
  writes nothing); `change` lists its new / resolved findings. History from databases built
  before incremental ingest is kept by the migration.

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

Rule-based findings are fully stable. AI findings are stable while their inputs are unchanged:
with incremental ingest, rows of unchanged files keep their ids, so the verifier's prompts for
components that did not change are byte-identical and the response cache replays the same answers.

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
| `run` | one row per ingest that changed the database: commit, mode, counts, baseline run |
| `change` | findings new / resolved in each run compared with its baseline |
| `stage_state` | commit the data represents, extraction signature and completion per stage |
| `extraction_unit` | per unit (file or cross-source step): blob id, signature and context it was built from |
| `unit_fact`, `component_decl` | stored extraction context and component declarations per unit |
| `ingest_meta` | repository identity (root commit, remote URL without credentials) |

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
Options: `--repo-url`, `--repo`, `--db`, `--no-llm`, `--workers`, `--show-unresolved`, `--label`,
`--stats`. Exit code 0 = requested stages complete, 1 = error (database unchanged), 3 = model
stage requested but incomplete (deterministic results are published).

### `gitsnap.py`: read-only Git access
Repository identity (root commit), HEAD, tracked files with blob ids (`ls-tree`), blob contents
(`cat-file --batch`), commit diffs and the clean-checkout check. It never writes to the checkout.

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

### `mcp/server.py`: the interface for IDE agents
An MCP server (stdio, or HTTP with `--http`) that exposes the `query.py` functions as tools:
`status`, `context`, `impact`, `component`, `conflicts`, `risks`, `decisions`, `changes`, `search`,
`read_source` and `sql`. `context(name)` is the one-call entry point: it assembles, without any model
call, a component's relationships (implemented apart from documented and outdated), governing decisions,
document claims with their perspective, linked findings with the basis of each link, recorded gaps, the
snapshot/stage state and the cited source passages. It fits a size budget by dropping passages first, then
shortening lists, and reports what it left out (JSON stays valid). `focus` only reorders; it never hides. This is how Cursor / Claude Code use the Context Service. It is
read-only, opens `context.db` per call, and caps outputs for the agent's context window.
Configured in `.cursor/mcp.json` and `.mcp.json`.

### `ask.py`: CLI fallback (questions in plain language)
```sh
python3 ask.py "If I change how order-service stores money, what breaks and which docs are wrong?"
python3 ask.py --show-tools "Where is the auth model weak?"
```
A Sonnet agent with tools: the five `query.py` functions, `read_source` and read-only `sql`.
It answers as claim, `path:line`, confidence, with a *Watch out* section. Afterwards,
`check_citations()` flags any cited file that is not in the repository.

### Generated files (git-ignored)
- `context.db` (+ `-wal`/`-shm` while open): the knowledge base, updated in place per commit.
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
  - Incremental ingest: see *Limitations* under [Keeping context.db up to date](#keeping-contextdb-up-to-date).
- **Next steps:**
  - Run `ingest.py` from CI or a post-merge hook, so the database follows the default branch.
  - Multiple repositories in one knowledge base, for cross-repo relationships.

---

## Keeping context.db up to date

### Setup and update

```sh
python3 ingest.py --repo ../att-hackathon-task            # first run: full build
git -C ../att-hackathon-task pull                         # ...the source moves on...
python3 ingest.py --repo ../att-hackathon-task            # incremental update
python3 ingest.py --repo ../att-hackathon-task --no-llm   # rules only; enrichment can follow later
python3 -m unittest discover -s tests -v                  # tests (temporary repos, mocked model)
```

The input checkout must have no staged or unstaged changes to tracked files; otherwise ingest
stops before touching the database and says what to commit or stash. Untracked files are not
part of the corpus. Without `--repo`, a managed clone in `.cache/sources/<repo>` is pulled and used.

### Provenance: commit and blob ids
Git is the only identity system. `ingest_meta` stores the repository identity (root commit,
plus the credential-free remote URL); a database built from another repository is refused.
`stage_state` stores, per stage (`deterministic`, `model`), the commit the data represents, the
extraction signature and whether the stage is complete. `source.blob` stores the Git blob id
each file was read from. Every knowledge row carries `unit`: the extraction unit that produced
it, and `extraction_unit` records the blob, signature and shared-context digest each unit was
built from. So for any fact: evidence → file:line → blob id → commit.

### What "incremental" means here
| unit | what | re-run when |
|---|---|---|
| `det:<path>` | file-local rules: manifests, routes/calls/SQL, API specs, doc mentions, security rules | its blob changes, the extraction signature changes (`EXTRACTION_VERSION`, PyYAML availability), or the shared context it reads changes (components, aliases; ports for code and specs) |
| `det:*` | cross-source results: component discovery, addresses, compose/Kubernetes, migration replay, CODEOWNERS, component kinds, all `check_*`, agent-instruction files | any input text changes or an LLM code unit changes (recomputed globally from the stored endpoint/call facts of unchanged files) |
| `llm:code:<path>` | AI code pass for one file | its blob, model id or prompt changes, or the resolution context (components, aliases, ports, extraction signature) changes |
| `llm:<path>` | prose and diagram extraction | its blob, model id, prompt text or the component catalog changes |
| `llm:diagram-check` | diagram arrows vs observed call graph | knowledge changed (no model call: uses the stored diagram output) |
| `llm:verify` | verifier verdicts + contradictions | anything it reviews changed |

- Change detection is `git diff --name-status --no-renames <stored commit> HEAD` (renames are a
  delete plus an add), cross-checked against the stored blob ids.
- Unchanged files are not re-read by extractors; their rows stay untouched (same ids).
- A re-extracted unit is matched against its previous rows by natural key: identical records keep
  their id, vanished ones are deleted, new ones inserted. Repeated runs never duplicate rows.
- De-duplication is per file: a file's results never depend on which other file reported the
  same route or call first. The cross-source checks still see each key once, in full-run order,
  so their results equal a full rebuild. The AI code pass skips what the rules already found in
  the same file.
- Components exist while at least one unit declares them (`component_decl`); a library used by a
  deleted manifest disappears only if no other manifest uses it. The AI's document type refines
  `source.kind`; the rule-based one stays in `source.det_kind`, which is what change detection uses.
- A commit that changes only unprocessed files (lockfiles, binaries) just advances the snapshot.
  The same commit, signature and completed stages: exit 0 with no writes.

### Stages and invalidation
`--no-llm` publishes the deterministic stage and records the model stage as `incomplete` with
what is pending. A later normal run at the same HEAD performs only the missing enrichment
(code pass first; when its results change, the cross-source checks are recomputed with them, as
in a full run). Model results for changed or deleted files, or produced with another catalog or
resolution context, are removed together with the source update (old model output is never shown
next to a new snapshot); changing `EXTRACT_MODEL`, `REASON_MODEL` or a prompt re-runs exactly the
affected units. Missing credentials or failed model calls are reported, the model stage stays
`incomplete`, ingest exits with 3, and the next run retries only what is missing. Signatures
contain model ids and prompt digests, never credentials.

### Migration and failure behaviour
- The schema is versioned (`PRAGMA user_version` = 3; v2 -> v3 adds `claim.perspective` in place,
  without re-extraction; the deterministic extraction signature is unaffected). Databases from before incremental ingest
  (with or without fingerprints and run history) are migrated in place and get one full logical
  refresh, in the same transaction; their run history is kept.
- Two transactions, in WAL mode: (1) migration + deterministic update + removal of stale model
  results + integrity check (no dangling evidence, component or source references; search index in
  step with sources) + advancing the deterministic marker + run record; (2) enrichment + advancing
  the model marker + run record. A crash or error rolls back the open transaction, so readers
  (query.py, ask.py, the MCP server) see the previous or the new state. A newly created database is
  deleted if its first build fails.

### Limitations
- The shared-context dependency is coarse: a port-registry change re-extracts every code and spec
  file and re-runs the AI code pass for every code file (identical prompts, so the response cache
  answers them), and any catalog change (new component, table or team) re-runs prose and diagram
  extraction for every document, because the catalog is in the prompt.
- Cross-source checks are recomputed globally whenever any input changes.
- Verification is re-run as a whole after any knowledge change; unaffected components are cache
  hits, but a removed cache means paying for them again.
- Only the current snapshot is kept: no as-of queries, no dirty-tree ingestion, no rename tracking.
- Bump `EXTRACTION_VERSION` (ingest.py) / `MODEL_STAGE_VERSION` (enrich.py) when changing extractor
  or model-stage code; prompt text and model ids are tracked automatically.
- Symlinks and submodules are not part of the corpus (only regular tracked files).
