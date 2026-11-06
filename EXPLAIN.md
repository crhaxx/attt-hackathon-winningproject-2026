# How the Context Service works: explained for everyone

This page explains the whole system without assuming you can code. Each part has a plain
explanation first. The small grey text under it is for technical readers and says what
actually happens under the hood. You can skip it.

---

## The problem, in one picture

Imagine a big company's knowledge as a **huge, messy library**:

- the **programs** themselves (the code that actually runs)
- **instruction manuals** and **wiki pages** that people wrote, some of them years ago
- **meeting notes** where decisions were made
- **incident reports** about the times something broke
- **drawings** of how the systems connect
- **settings files** that say how everything is started and wired together

The trouble is that these sources **disagree with each other**. A manual says one thing, the
program does another, and a drawing shows a connection that was removed months ago. Nobody
knows which source is right.

Now picture an **AI assistant** that writes code for the company's engineers. If it reads the
wrong manual, it writes wrong code. If it doesn't know about a known security hole, it copies
the hole into new code.

<sub>⚙️ For the hackathon, the "library" is one GitHub repository
([arkik7/att-hackathon-task](https://github.com/arkik7/att-hackathon-task)) with 373 files:
TypeScript, Python and Java code, OpenAPI specs, SQL migrations, Markdown wikis, ADRs, meeting notes,
incidents, PNG diagrams, docker compose and CI config. The mistakes in it are deliberate.</sub>

## What we built, in one sentence

**A robot librarian that reads the whole library by itself, writes down every fact *together
with where it found it*, spots where sources contradict each other and where the security
holes are, and then answers the AI assistant's questions with proof.**

<sub>⚙️ An automated ETL (extract-transform-load) pipeline. It builds a knowledge graph in one SQLite
database (`context.db`) and serves it to IDE agents (Cursor, Claude Code) over MCP, the Model Context
Protocol.</sub>

---

## The chain, step by step

```
  The library            The robot librarian                        The notebook        The assistant
  (the repository)                                                  (the database)      (AI in the editor)

  ┌───────────┐   1 copy   2 sort   3 read the     4 AI reads   5 compare   6 security
  │ code      │  ───────► ───────► exact forms ──► the code ──► everything ─► inspector ─┐
  │ manuals   │                                                                         │
  │ notes     │   7 AI reads documents    8 AI looks at     9 the judge                 │
  │ drawings  │  ◄──────────────────────── drawings ◄─────── double-checks ◄────────────┘
  │ settings  │            │
  └───────────┘            ▼
                    ┌─────────────────┐       11 asks questions      ┌────────────────┐
                    │  notebook with  │  ◄───────────────────────── │  AI coding     │
                    │  every fact +   │  ──────────────────────────►│  assistant     │
                    │  its "receipt"  │      answers with proof      │  (in the IDE)  │
                    └─────────────────┘                              └────────────────┘
                     10 kept up to date: only changed pages are re-read
```

### Step 1: Make a copy of the library (and never touch the original)

The robot makes its own copy of the library to read from. It **never writes anything into
the original**, so nothing the company owns can be damaged.

<sub>⚙️ `ingest.py` runs `git clone` / `git pull` into `.cache/sources/<repo>/`. `gitsnap.py` gives read-only Git
access: the commit being analysed and each file's blob id, which is later used to see what changed. The source
repository is never modified.</sub>

### Step 2: Sort every file onto the right shelf

The robot looks at each file and decides what it is: a program, a manual, a meeting note, a
settings file, a drawing, and so on. It decides by the file's **name and content**, the way you'd
recognise a recipe by its look, not by which drawer it happens to be in. That way it works on
*any* company's library, however it's organised. It also sets aside:
- **copies a machine generated automatically**, which tell nothing about human intentions
- **files in an "archive" folder**, which are marked as *old*

<sub>⚙️ `classify()` uses standard file names (`package.json`, `Dockerfile`, `CODEOWNERS`), extensions and
content sniffing: `openapi:` → API spec, `apiVersion:` + `kind:` → Kubernetes. Generated code is detected by
`DO NOT EDIT` / `@generated` markers and protoc file names. Prose starts as `doc`, and the AI later gives it a
real type (wiki, ADR, incident, …). There are no folder-name rules.</sub>

### Step 3: Read the "exact forms" with exact rules (no AI)

Some files are like **official forms**: always the same layout, so they can be read with total
certainty. From them the robot learns, without guessing:
- **which programs exist**, and which program needs which
- **who talks to whom**: "the shop program calls the user program at this address"
- **who owns what**: which team is responsible for which program
- **what the database really looks like today.** The robot replays the history of database changes from the
  very first one, like replaying a game of chess move by move to see the final board.

These facts get the **highest trust score**, because they're read from what actually runs.

<sub>⚙️ Parsers for npm/Python/Go/Maven/Gradle/Cargo manifests, compose and Kubernetes YAML (PyYAML),
OpenAPI, CODEOWNERS (git's last-match-wins rule), framework route patterns (Express, FastAPI, Spring, Go,
ASP.NET) and env-configured service addresses. The SQL migrations are replayed in an in-memory SQLite to get
the real final schema and every dropped or renamed column. Confidence 0.85–0.95, status `observed`. Addresses
are matched to components by env var names, host names, unique ports and case/separator-insensitive names. If
no single component matches, the call stays unresolved: it is never guessed.</sub>

### Step 4: An AI reads the programs themselves

Code comes in many languages and styles, and fixed rules can't understand them all. So an AI
reads each program and reports what it offers, whom it calls, what data it uses, and any
**security problems** it sees. For example: *"this page shows any customer's order to anyone
who knows the order number"*.

**The safety catch:** the AI must quote the exact line it's talking about. If that line isn't
really in the file, its answer is **thrown away**. That stops the AI from making things up.

<sub>⚙️ `enrich.code_pass()` sends each hand-written code file to `EXTRACT_MODEL` via OpenRouter. It asks for
JSON with routes, outbound calls, SQL tables, events and risks, each with a verbatim quote. `locate_quote()`
discards anything whose quote isn't in the file. Confidence 0.65–0.75, below the deterministic parsers.</sub>

### Step 5: Compare everything (the detective)

Now the robot lays all the facts side by side and looks for things that don't add up:
- the manual promises a feature that the program doesn't have
- one program calls another at an address that doesn't exist, so it would break
- a wiki page describes a database column that was removed long ago
- a program depends on another one, but the start-up settings don't say so
- a program has **no responsible team**, so nobody would get the call when it breaks
- **a shared building block's "shop window" is wrong.** A package advertises parts that nobody
  uses, or points to files that don't exist
- **the shared settings templates have drifted.** A template nobody uses, a settings file that
  points to a missing file, or a project that ignores the shared template everyone else follows
- **one project checks its work with a different tool than the others.** Its "check for mistakes
  before shipping" step calls another program, so its checks don't match the rest

<sub>⚙️ `check_endpoints`, `check_deploy_vs_code`, `check_schema_drift`, `check_ownership` and
`check_package_surface` write `conflict` rows (kind conflict / gap / stale). Each one points to all of its
evidence. The package-surface checks: `unused-package-export` (a `package.json` `exports` entry that no file
imports, or that points to a missing file); `tsconfig-missing-file` (a tsconfig `extends`/`references` target
that isn't in the repo); `unused-tsconfig-preset` (a shared TypeScript preset nothing extends);
`tsconfig-ignores-shared` / `tsconfig-not-shared` (a project inlining its own compiler options instead of
extending the shared config); `typecheck-entrypoint` (a `check-types`/`typecheck` script that calls a
different compiler than the other projects).</sub>

### Step 6: The security inspector

A second pass looks for well-known dangerous patterns:
- passwords written directly into files
- doors open to every website on the internet
- trusting whatever the visitor *says* about who they are
- database commands glued together from user input

Each finding comes with a **"do this instead"** instruction, so the assistant doesn't repeat it.
The robot is also wary of files that try to **give orders to AIs**. It flags them and never obeys them.

<sub>⚙️ Regex rules (`RULES` in `ingest.py`): hardcoded secrets, CORS `*`, identity from client headers, SQL
interpolation, error-detail leaks, TLS off, sensitive data in logs. Comment lines are ignored. Files like
`AGENTS.md` / `.agents/` are flagged as a prompt-injection vector.</sub>

### Step 7: An AI reads the human-written documents

Manuals, wiki pages, meeting notes and incident reports are written in normal language,
which only an AI can really understand. For each document it notes:
- **what kind of document it is**, and whether it is current, historical or just a proposal
- **what it claims**: "orders show the total in dollars"
- **decisions and rules** people agreed on, and whether each is *really approved* or only proposed
- **risks** the document mentions

The same safety catch applies: every item needs a real quote. On top of that, **a document can
never outrank the program itself**. Its trust score is capped lower.

<sub>⚙️ `extract_prose()` uses `EXTRACT_MODEL`. It returns doc type and status, facts (a closed set of
relation types), claims, risks, and decisions/policies/constraints with a status (accepted / proposed /
superseded / rejected) linked by `GOVERNS` to the components they apply to. Prose confidence is capped at 0.7.
See [ONTOLOGY.md](ONTOLOGY.md).</sub>

### Step 8: An AI looks at the drawings

Architecture drawings are images. An AI that can see pictures turns each one into a list of
boxes and arrows. The robot then compares the arrows with what the programs *really* do. An
arrow with no real connection behind it means the drawing is probably out of date.

<sub>⚙️ `read_diagrams()` sends the PNGs to `REASON_MODEL` (vision). Nodes are mapped to known components,
and the drawn edges are compared with observed calls: `diagram-edge-not-in-code` and
`code-call-missing-from-diagram`.</sub>

### Step 9: The judge double-checks

For every program, a stronger AI acts as a **judge**. It sees what the program really does, what
the documents claim (with their dates), which approved rules apply, and the suspicions from the
detective. It then:
- **confirms or rejects** each suspicion. Rejected ones are kept with the reason, so a human can check.
- **finds new contradictions**, and says which source is the current one and why: newer date, a later decision, or "the code wins".
- may only point to evidence it was actually shown.

Example it found: *"The rules say people may only see their own orders, but this page shows
any order to anyone."*

<sub>⚙️ `verify()` sends one prompt per component to `REASON_MODEL` with observed facts, dated claims,
governing decisions and candidate conflicts, all referenced by evidence id. Confirmed candidates get their
confidence ×1.3, rejected ones ×0.3, and new ones are stored as `llm-contradiction`. Ids the model wasn't
given are discarded.</sub>

### Step 10: Write it all into the notebook, with receipts

Everything ends up in one **notebook** (a database). The important part is that **every single
fact has a receipt**: which file, which line, the exact words, who found it (a rule or an AI),
and how sure we are. Every fact is also labelled:

| Label | Means |
|---|---|
| **observed** | seen in what actually runs: trust it for "how it works now" |
| **declared** | written in a document or drawing: it's what people *intended*, and may be outdated |
| **outdated** | from the archive or a superseded document: it was true once |

<sub>⚙️ One SQLite file. Tables: `source`, `component`, `relation`, `evidence`, `claim`, `conflict`, `risk`,
plus `run`/`change` (history) and `stage_state`/`extraction_unit` (provenance). Search uses an FTS5
full-text index. The full model is in [ONTOLOGY.md](ONTOLOGY.md).</sub>

### Step 11: Keep it up to date, cheaply

Libraries change every day. When the robot runs again, it only **re-reads the pages that
changed**. Everything else is reused, so updates are fast and cheap. After each run it reports
**what got better and what got worse**: "2 new problems, 1 fixed since last time".

AI answers are also kept, so asking the same thing twice costs nothing.

<sub>⚙️ Incremental ingest: only files whose Git blob changed are re-extracted, cross-source checks are
recomputed, and an unchanged snapshot is a no-op. Model responses are cached in `.cache/llm/` by request hash.
Findings get fingerprints (type + subject + evidence file/quote, line numbers left out), and `query.py changes`
shows new and resolved findings between runs.</sub>

### Step 12: Hand it to the AI assistant

Finally, the AI assistant that helps engineers write code can **ask the notebook questions**
while it works:
- *"If I change this program, what else breaks, and who owns it?"*
- *"Which manuals must I not trust?"*
- *"Which approved rules apply here?"*
- *"Which security mistakes must I avoid in this part?"*
- *"What got worse since yesterday?"*

Every answer comes **with its receipts**, so the assistant (and the engineer) can check it. Before
the assistant writes code for a program, it's told to look up that program's risks and rules
first, so it doesn't repeat known mistakes.

<sub>⚙️ `mcp/server.py` is a read-only MCP server (stdio or HTTP). Its tools are `status`, `context`, `impact`,
`component`, `conflicts`, `risks`, `decisions`, `changes`, `search`, `read_source` and `sql`. It's configured
for Cursor (`.cursor/mcp.json`) and Claude Code (`.mcp.json`), and outputs are capped for the agent's context
window. `ask.py` is a command-line fallback agent with the same tools.</sub>

---

## A real example from start to finish

1. **A wiki page** (from February) says: *"Orders show the total as a decimal number of dollars,
   for example 25.00."*
2. **The database history** shows that, later, the "total" column was removed and replaced
   by "total_minor", an amount in cents (2500 = $25.00).
3. **An approved decision record** from March says money is stored in cents.
4. The **detective** notices that the wiki describes a column that no longer exists.
5. The **judge** confirms it: the wiki is older than the change and was never updated.
6. An AI assistant asked to "show the order total" now gets: *"Use `total_minor` (cents). Don't
   trust `wiki/orders-client.md`, it's out of date"*, with the receipts attached.

Without the Context Service, an assistant that trusted the wiki could have used a field that no
longer exists, or shown $2500.00 instead of $25.00.

<sub>⚙️ Evidence: `wiki/orders-client.md:5`, `apps/order-service/migrations/002-minor-units.sql:3`
(`ALTER TABLE orders DROP COLUMN total;`), `docs/adr/002-minor-units.md`. Conflict
`doc-describes-dropped-column`, verdict `confirmed`, confidence 0.85.</sub>

---

## Why you can trust the answers

| Promise | How it's kept |
|---|---|
| **No fact without a receipt** | every fact stores the file, line and exact words it came from |
| **The AI can't make things up** | AI answers are thrown away if their quote isn't really in the file |
| **What runs beats what's written** | facts from programs get higher trust than facts from documents |
| **No guessing** | if it's unclear which program something refers to, no link is made |
| **No taking orders from files** | text inside the library is treated as information, never as instructions |
| **Not tied to one library** | tested unchanged on a completely different company's code (Google's microservices demo) |

<sub>⚙️ These are the closed-vocabulary, quote-check, trust-order, no-guess and untrusted-data validation
gates in [ONTOLOGY.md](ONTOLOGY.md). The second repository was
[GoogleCloudPlatform/microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo): five
languages, gRPC and Kubernetes.</sub>

## What it can't do (yet)

- It reads **one library at a time**. Connecting several libraries is the next step.
- Some connections in code go through so many hands that the robot can't tell for sure where
  they lead. It then says "unknown" rather than guessing.
- The AI parts are good but not perfect. A few of the judge's findings can be overly strict,
  so they are labelled as AI findings and come with their reasons.
- It understands one common database language. Others are skipped with a note.

<sub>⚙️ Single repo per database. Unresolved calls are listed by `--show-unresolved`. Model quality depends on
`EXTRACT_MODEL` / `REASON_MODEL`. Migration replay only handles SQLite-compatible SQL. `.proto` contracts are
indexed but not yet parsed into endpoints.</sub>

---

## Mini-glossary

| Word | Plain meaning |
|---|---|
| **Repository** | the library: a folder holding all of a project's files and its history |
| **Code / program** | the instructions a computer actually runs |
| **Database** | an organised notebook the computer can search quickly |
| **AI model** | the "brain" that reads text or pictures and answers in words |
| **AI coding assistant** | an AI inside the engineers' editor that writes and changes code |
| **MCP** | a standard "plug" that lets the AI assistant ask our notebook questions |
| **Evidence / receipt** | file + line + exact words that prove a fact |
| **Confidence** | how sure we are, from 0 (not at all) to 1 (certain) |
| **Conflict** | two sources that disagree, or something important that's missing |

For the technical details, see [README.md](README.md), [WORKFLOW.md](WORKFLOW.md) and [ONTOLOGY.md](ONTOLOGY.md).
