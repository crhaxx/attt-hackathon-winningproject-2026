# Context Service: evidence-backed engineering knowledge

> AT&T Hackathon 2026: *Efficient AI Context Service for Enterprise Engineering Knowledge*

Engineering knowledge is scattered: code, specs, wikis, ADRs, meeting notes, incidents,
diagrams and deployment files, and those sources often contradict each other. This project is
an **automated pipeline** that reads such a repository (read-only), builds one knowledge base
out of it, and answers engineers' and AI coding assistants' questions. Every answer carries a
**claim + source (`file:line`) + confidence**.

It answers questions like:

- **"If I change X, what breaks?"** It traces dependents across code, configs and deployment
  manifests, and returns the owner, the data the component owns and the docs to update.
- **"Where do the sources contradict each other?"** For example, a wiki still documents a
  column a migration removed, or a diagram shows a connection that doesn't exist in the code.
  It also says which source is current and why.
- **"What security problems must generated code not repeat?"** For example, identity taken
  from a client header, a missing ownership check, or SQL built by string interpolation.
- **"What changed since the last ingest?"** New and resolved findings, run to run.

```
$ python3 ask.py "If I change how order-service stores money, what is affected and which docs are wrong?"

Changing how `total_minor` is stored would affect the inventory service, the payment service, the gateway,
the clients and several specs ...
- It takes `total_minor` from inventory's `/v1/availability` response ... - `apps/order-service/src/index.ts:85-112` - high
- `wiki/orders-client.md:5` says orders expose `total` as decimal dollars - STALE, 0.85, confirmed.
  Migration 002 dropped `total` (`apps/order-service/migrations/002-minor-units.sql:3`).
Watch out: GET /v1/orders/{id} may lack the customer ownership check that the list route applies ...
```

---

## Quick start

**Requirements:**
- Python **3.10+**
- `git`
- **PyYAML** ≥ 6 (`pip install "pyyaml>=6"`), which parses compose, Kubernetes and OpenAPI YAML
- An **OpenRouter API key** for the AI stages (optional: without one, the deterministic stage still runs)

```sh
pip install "pyyaml>=6"
echo "OPENROUTER_API_KEY=sk-or-..." > .env      # git-ignored; or export it in your shell

python3 ingest.py                               # clone the source repo and build context.db
python3 ask.py "Where do the docs contradict the code?"
```

On NixOS: `nix-shell -p python3 python3Packages.pyyaml git --run 'python3 ingest.py'`

### Configuration

| Variable | Purpose |
|---|---|
| `OPENROUTER_API_KEY` | Enables the AI stages. Read from the environment or a local `.env` |
| `EXTRACT_MODEL` | Cheap model for per-file extraction (default in `llm.py`) |
| `REASON_MODEL` | Model for diagrams, verification and Q&A (default in `llm.py`) |
| `CONTEXT_DB` | Database for `query.py` / `ask.py` (default `context.db`) |

---

## How it works

```
any git repo ──► collect ─► classify ─► extract (parsers) ─► AI code pass ─► cross-check ─► security scan
                                                                                               │
          context.db ◄── verify (AI) ◄── diagrams (vision AI) ◄── prose → facts (AI) ◄─────────┘
              │
              ├── query.py   fixed questions, no AI, deterministic
              ├── ask.py     natural-language agent over the same tools
              └── run diff   new / resolved findings since the last ingest
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
5. **Read prose and diagrams.** The AI turns docs into claims and diagrams into boxes and arrows.
6. **Verify.** For each component, the AI compares code facts with dated doc claims. It
   confirms or rejects candidates, finds new contradictions, and says which source is current.

### Why the answers can be trusted

- **Every fact has evidence:** file, line, exact quote and the extractor that produced it.
- **Anti-hallucination gate:** an AI-extracted item is kept only if its quote really occurs in the file.
- **Trust order:** code and config (`observed`) beat specs and docs (`declared`), which beat
  archive and history (`outdated`). AI facts are capped below parser facts.
- **No evidence, no link.** A call whose target can't be resolved uniquely stays unresolved
  instead of being guessed. The same API name in two projects is not a relationship.
- **Repository text is data, never instructions.** The repo contains AI-agent instruction
  files, and they are flagged as a prompt-injection risk.
- **Cached AI answers** (`.cache/llm/`): reruns are fast, cheap and repeatable.

### Tested beyond the target repo

The same code, unchanged, was run on
[GoogleCloudPlatform/microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo).
That repo uses five languages, gRPC instead of REST, and Kubernetes instead of compose. The
pipeline found all 11 services, the real call graph, owners, diagrams and security risks, and
`ask.py` answered questions about it with sources.

---

## Usage

```sh
# build the knowledge base
python3 ingest.py                                  # default: arkik7/att-hackathon-task
python3 ingest.py --repo-url <any git url> --db other.db
python3 ingest.py --no-llm                         # deterministic stage only (free)
python3 ingest.py --show-unresolved                # call targets that could not be attributed

# ask in natural language (AI agent with tools, cites file:line)
python3 ask.py "Which services break if user-service changes its API?"
python3 ask.py --show-tools "What security risks should generated code avoid?"

# fixed questions (no AI)
python3 query.py impact user-service       # dependents (transitive), owner, data, docs, conflicts, risks
python3 query.py conflicts total           # contradictions / gaps / stale docs with verifier verdicts
python3 query.py risks order-service       # security findings + "do instead" guidance
python3 query.py component api-gateway     # everything connected to one component
python3 query.py search "minor units"      # full-text search over every file
python3 query.py changes                   # new / resolved since the previous ingest
```

The knowledge base is a single SQLite file. You can browse it with
[DB Browser for SQLite](https://sqlitebrowser.org/): `sqlitebrowser context.db`.

| Table | Contents |
|---|---|
| `source` | every file: type, dates, status, summary, text (+ full-text index) |
| `component` | services, libraries, endpoints, tables (with live columns), teams |
| `relation` | `src TYPE dst` with status (observed / declared / outdated) and confidence |
| `evidence` | file, line, quote and extractor behind every fact |
| `claim` | what each document asserts about a component |
| `conflict` | conflicts / gaps / stale docs, with verifier verdict and rationale |
| `risk` | security concern, severity, guidance for generated code |
| `run`, `change` | ingest history and the new / resolved findings per run |

---

## Project structure

| File | Role |
|---|---|
| `ingest.py` | The pipeline: collect, classify, parse, cross-check, security scan, run diff |
| `enrich.py` | AI stages: code pass, prose extraction, diagrams, verification |
| `llm.py` | OpenRouter client with retries and an on-disk response cache |
| `query.py` | Deterministic queries over `context.db` |
| `ask.py` | Natural-language agent over the query tools, with citation checking |
| `WORKFLOW.md` | Detailed walkthrough of every step, design decisions and limits |

## Limits and next steps

- **Limits:**
  - Migration replay only understands SQLite-compatible SQL.
  - gRPC `.proto` files are indexed but not yet parsed into endpoints.
  - Calls through variables with no address evidence stay unresolved (by design).
- **Next steps:**
  - An MCP server, so IDEs can call `ask`, `impact` and the other tools directly.
  - Stable evidence ids and incremental per-file re-ingest on every commit.
  - Multiple repositories in one knowledge base, for cross-repo relationships.

See **[WORKFLOW.md](WORKFLOW.md)** for the full explanation.
