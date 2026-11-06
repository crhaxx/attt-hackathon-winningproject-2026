# Ontology

The Context Service stores engineering knowledge as a **typed, evidence-backed graph** in one
SQLite file (`context.db`). This document is the predefined ontology the pipeline classifies,
maps and validates against. Every entity and relation type listed here is the closed set the
code accepts. An AI output with any other type is dropped.

```
                 ┌──────────── evidence (file, line, quote, extractor) ────────────┐
                 │                         backs every                              │
   source ──► component ──relation──► component        claim     conflict     risk
   (file)     (service, library,      (typed, with      (doc      (conflict/   (security,
              endpoint, table, team,   status +          assertion) gap/stale)  guidance)
              decision, policy, ...)   confidence)
```

## How it covers the brief's perspectives

| Perspective (brief) | Represented as |
|---|---|
| **Systems** | `component` of kind `service`, `library`, `project`, `endpoint`, `table` |
| **Concepts** | `component` of kind `concept`: things docs talk about that are not code components (e.g. *warehouse console*) |
| **Policies** | `component` of kind `decision`, `policy`, `constraint`, with a status, the rule text, and `GOVERNS` links |
| **Relationships / dependencies** | `relation` rows (closed set of types below) |
| **Risks** | `risk` rows (security) and `conflict` rows (contradictions, gaps, stale sources) |
| **Evidence** | `evidence` rows: every relation, claim, conflict, risk and decision points to one |
| **Ownership** | `team` components and `OWNED_BY` relations |
| **Time / currency** | `source.doc_date`, `source.git_date`, `source.doc_status`, decision status, run history |

---

## Entities

### `source`: one row per file
| Field | Meaning |
|---|---|
| `kind` | Source type: `code`, `test`, `generated`, `config`, `dependency_manifest`, `deployment`, `ci`, `api_spec`, `api_contract`, `schema`, `fixture`, `ownership`, `diagram`, `agent_instructions`, `lockfile`, `binary`, and prose types decided by the AI from content: `readme`, `wiki`, `adr`, `design_doc`, `meeting_note`, `incident`, `issue`, `release_note`, `runbook`, `api_doc`, `how_to`, `doc` |
| `archived` | Under an archive/legacy/deprecated/history folder: its facts become `outdated` |
| `doc_date`, `git_date` | Date stated in the file, and last commit date. Used to decide which source is newer |
| `doc_status` | `current`, `historical`, `proposal`, `superseded` or `unknown` (AI) |
| `summary` | One-line AI summary. For diagrams, the extracted nodes and edges |

### `component`: things in the system
| Kind | Id format | Created by |
|---|---|---|
| `service` | `comp:<name>` | a manifest + it serves routes or listens on a port |
| `library` | `comp:<name>` | a manifest + other components depend on it |
| `project` | `comp:<name>` | a manifest, neither of the above |
| `endpoint` | `endpoint:<service> <METHOD> <path>` | routes in code, OpenAPI specs |
| `table` | `table:<service>.<table>` | migration replay (with live columns), SQL in code |
| `team` | `team:<handle>` | CODEOWNERS, docs |
| `library-ext` | `lib:<name>` | external dependencies in manifests |
| `topic` | `topic:<name>` | events published/consumed (AI code pass) |
| `decision` | `decision:<slug>` | ADRs, agreed meeting outcomes (AI, from prose) |
| `policy` | `policy:<slug>` | rules that must always hold, e.g. *never write another service's database* |
| `constraint` | `constraint:<slug>` | limits, e.g. *minor units only, no multi-currency* |
| `concept` | `concept:<slug>` | anything else docs refer to that matches no component |
| `unknown-service` | `spec-only:<name>` | an API spec whose service could not be identified |

Decisions, policies and constraints carry `props.status`:
- `accepted`: in force
- `proposed`: intent only
- `superseded` / `rejected`: no longer in force
- `unknown`: the document doesn't say

They also carry `props.rule` (the normative statement), `props.date` and `props.supersedes`
(the title of the decision they replace, matched by title at query time). Their source document
and quote are recorded by a `STATED_IN` relation, so provenance follows incremental re-ingest.

Component names have **aliases**: the manifest name, the folder name, and compose/Kubernetes
service names. Matching ignores case and separators. A name claimed by two components is marked
ambiguous and is never used to link anything.

### `claim`: what a document asserts
`component, topic (api|schema|money|auth|deployment|dependency|ownership|ops|other), text, path,
doc_date, doc_status, evidence_id, perspective`. Claims are **compared against observed facts** in the
verification step. They are never treated as true just because they are written down.

`perspective` is how the *sentence* presents the statement, because one page often mixes the present,
plans and open ideas: `current` (how things are now), `intended` (agreed target or plan, e.g. "will use"),
`proposed` (not decided, e.g. "has not reached a launch date"), `historical`, `reported` (incident or
ticket observation), `unknown` (not stated, and every claim extracted before schema v3). A `current`
sentence inside a superseded or historical document is stored as `historical`. Facts from prose carry the
same value in `relation.props.perspective`; `impact` does not treat `intended`/`proposed` edges as
dependencies, and `historical` ones are `outdated`.

### `conflict`: a problem between sources
| `kind` | Meaning |
|---|---|
| `conflict` | two sources disagree: code vs spec, doc vs doc, diagram vs code, code vs an accepted decision |
| `gap` | something that should exist is missing: owner, deployment dependency, spec entry |
| `stale` | a source describes something that no longer exists |

Fields:
- `check_name`: which rule or AI step found it
- `evidence_ids`: all supporting evidence
- `verdict`: `confirmed` / `rejected` by the AI verifier, or `llm-found`
- `rationale`
- `fingerprint`: a stable identity across runs

### `risk`: a security concern
Fields:
- `rule`
- `severity`: `high` / `medium` / `low`
- `subject`
- `description`
- **`guidance`**: what generated code must do instead
- `confidence`, `evidence_id`, `fingerprint`

### `run`, `change`: history
One `run` row per ingest, plus the conflicts and risks that are **new** or **resolved**
compared with the previous comparable run.

---

## Relations (closed set)

| Type | From → To | Produced by | Typical status |
|---|---|---|---|
| `DEPENDS_ON` | component → component | manifests (internal deps), prose | observed / declared |
| `USES_LIBRARY` | component → library-ext | manifests | observed |
| `CALLS` | component → component (`props.method`, `props.path`) | env-configured addresses in code, URL literals, AI code pass | observed |
| `CONFIGURED_TO_CALL` | component → component | compose / Kubernetes env vars | observed |
| `DEPLOY_DEPENDS_ON` | component → component | compose `depends_on` | observed |
| `EXPOSES` | component → endpoint | route patterns, AI code pass | observed |
| `DECLARES` | component → endpoint | OpenAPI / Swagger specs | declared |
| `HAS_TABLE` | component → table | migration replay | observed |
| `READS` / `WRITES` | component → table | SQL in code, AI code pass | observed |
| `PUBLISHES_EVENT` / `CONSUMES_EVENT` | component → topic | AI code pass, prose | observed / declared |
| `OWNED_BY` | component → team | CODEOWNERS (last matching rule wins), prose | observed / declared |
| `GOVERNS` | decision/policy/constraint → component | prose | declared (outdated if superseded) |
| `STATED_IN` | decision/policy/constraint → source | prose (where it was written, with quote) | declared |
| *supersedes* | decision → decision | `props.supersedes`, resolved by title at query time | declared |
| `DIAGRAM_EDGE` | component → component | vision model over diagrams | declared |
| `MENTIONED_IN` | component → source | alias match in docs | declared |
| `KNOWN_AS` | component → source | prose: another name the document itself ties to the component (e.g. "Edge, the API gateway"); `props.alias`, quote-checked, rejected if the name already means another component | declared |
| `AFFECTED_BY_INCIDENT`, `MIGRATES_TO`, `REPLACES`, `DEPRECATES`, `DECIDED_BY`, `SHARES_DATA_WITH` | component → component/concept | prose | declared / outdated |

---

## Status: how far to trust a fact

| Status | Source | Meaning |
|---|---|---|
| `observed` | code, config, manifests, migrations, deployment files | ground truth for **current behaviour** |
| `declared` | specs, docs, diagrams, decisions | **intent**: may be stale |
| `outdated` | archived/history sources, superseded or historical documents | was true once |

## Confidence: how sure the extraction is

| Range | Typical source |
|---|---|
| 0.9–0.95 | deterministic parsers: manifests, compose, CODEOWNERS, migration replay, OpenAPI |
| 0.75–0.85 | framework route patterns, env-configured calls, SQL in code |
| 0.65–0.75 | AI code pass (quote-checked) |
| ≤ 0.7 | AI prose extraction: **capped**, so a document never outranks code |
| 0.4 | a component is merely mentioned in a doc |

The verifier adjusts conflict confidence: confirmed ×1.3 (max 0.95), rejected ×0.3. Archived
sources get 0.7× on risks.

---

## Validation gates (what keeps the graph trustworthy)

1. **Closed vocabulary.** Relation types, entity kinds, statuses, severities and decision
   statuses must come from the sets above. Anything else is dropped.
2. **Quote check.** Every AI-extracted item must quote the source, and the quote must really
   occur in the file (whitespace/markdown-insensitive). If it doesn't, the item is dropped.
3. **Evidence-only verification.** The verifier may cite only evidence ids it was given.
4. **No guessing links.** A call target is linked only if exactly one component matches it.
   Unresolved ones are reported (`--show-unresolved`), and ambiguous aliases are never used.
5. **Generated code is not evidence.** protoc/openapi-generator output is skipped.
6. **Repository text is data.** Every prompt says so, and agent-instruction files in the corpus
   are flagged as a prompt-injection risk.

## Storage, indexing and retrieval

- **Storage:** one SQLite file. Graph = `component` + `relation`. Provenance = `evidence`.
- **Indexes:** a full-text index (`source_fts`, FTS5) over every file, plus relation indexes on `src` and `dst`.
- **Retrieval:** graph traversal (`impact` follows `DEPENDS_ON`, `CALLS`, `CONFIGURED_TO_CALL`
  and `DEPLOY_DEPENDS_ON` backwards), filtered lookups (`conflicts`, `risks`, `decisions`),
  full-text search, and read-only SQL. All of these are exposed over **MCP** to IDE agents.
- **Why no vector database (yet):** questions here are about structure ("what depends on X",
  "what contradicts Y"), which a graph answers precisely. Full-text search covers fuzzy lookups.
  Embeddings would be the next addition for semantic search over very large prose corpora.
