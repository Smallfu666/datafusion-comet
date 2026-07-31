# AGENTS.md — Apache DataFusion Comet contribution lane

This directory is the private research and coordination lane for contributions to
[`apache/datafusion-comet`](https://github.com/apache/datafusion-comet).

The Comet source checkout is deliberately separate. Do not copy this directory into the
source tree, and do not commit research notes, prompts, handoffs, raw logs, machine paths,
or private infrastructure details to any contribution branch.

## Read order

Before doing any work:

1. Read `STATUS.md`.
2. Read `ISSUE_SELECTION_GATES.md` and `PUBLIC_DISCLOSURE_RULES.md`.
3. In the current Comet source checkout, read the upstream `AGENTS.md`,
   `CONTRIBUTING.md`, `docs/source/contributor-guide/index.md`, and the relevant
   development/testing guide.
4. If the task matches one of Comet's upstream `.claude/skills/`, read and follow that
   skill as repository-specific guidance.

The shared policies are the source of truth if this file conflicts with a local note.

## Strategic scope

Comet is the Rust/DataFusion side of the owner's Spark native-execution portfolio.
Gluten's Velox/C++ work is retained as technical history and transferable knowledge,
but its contribution lane is KILL under the current portfolio status. Do not restart
Gluten monitoring or new work unless the owner explicitly reopens it.

Gate 0 for Comet is satisfied by at least one of:

- Spark-to-native planner, fallback, or semantic-correctness expertise transferable
  from Gluten;
- Rust, Arrow, DataFusion, JNI/FFI, memory, shuffle, storage, or execution-runtime work;
- a production-scale CPU/HPC experiment that ordinary laptop testing cannot establish;
- a controlled cross-engine investigation that improves understanding without making
  unsupported "Comet versus Gluten" claims.

The RTX 3060 itself is not a Comet advantage. Treat that workstation as a development
host, not as evidence that GPU acceleration is involved.

## Public-action gate

Default state is **zero public writes**:

- no `take` comment;
- no issue or PR comment;
- no labels, assignment, review request, issue creation, or upstream PR;
- no maintainer tagging.

Comet documents `take` as its issue-assignment convention, but it is still a public
action and requires explicit owner approval. Local/fork research and neutral
internal-review branches are allowed.

## One-issue workflow

Work on one candidate at a time.

1. Pin the current upstream `main` SHA and record the checked-at timestamp.
2. Read the issue body, all comments, assignees, labels, linked events, and recent
   related commits.
3. Search open and merged PRs by issue number **and by mechanism keywords**.
4. Search Apache DataFusion and `datafusion-spark` where an upstream dependency or
   semantic change may already own the fix.
5. Check the Comet roadmap and recent maintainer activity for why-this, why-now.
6. Estimate scope and verification cost before building.
7. Reproduce on current main before changing production code.
8. Use vanilla Spark as the correctness oracle where applicable.
9. Prove whether the query is natively executed, partially executed, or falls back.
10. Record the exact suite/test count; process exit code alone is not sufficient.

Classify the result:

- **PROMOTE** — deterministic current-main repro, understood ownership, bounded patch,
  credible regression test.
- **PARK** — valid topic, but blocked by environment, owner/reporter evidence, active
  dependency, or unstable reproduction.
- **KILL** — duplicate, already fixed, cannot reproduce, wrong scope, or poor strategic
  fit.

Store the evidence in `research/`; never turn a weak candidate into a patch merely to
produce a PR.

## Implementation and independent review

Only after PROMOTE:

- use one implementer agent with a written hypothesis, scope, acceptance criteria, and
  test matrix;
- require a pre-fix reproduction and the smallest defensible change;
- commit/push only to a neutral `internal/...` branch on the private fork;
- do not use `Fixes`, `Closes`, or an upstream issue reference in internal commit
  messages;
- use a separate, fresh reviewer agent that does not modify code;
- reviewer pins base and head SHAs, reads tests first, and returns
  `GO`, `COMMENT`, or `REQUEST_CHANGES`;
- owner co-review is required before any upstream publication.

Review must cover Spark/DataFusion semantics, native/fallback boundaries, memory and
resource lifetime, exception paths, compatibility, repository standards, security, and
performance risk.

## Comet-specific build and runtime rules

- Build native Rust code before JVM tests when the upstream guide requires it.
- Do not use Maven `-pl`; Comet warns that it can create stale local artifacts and
  cross-worktree contamination.
- Run exact suites with the documented `-Dsuites` form. Never pass an empty
  `-DwildcardSuites`, which may run the full suite.
- Record debug/release mode, Spark profile, Java version, Rust toolchain, source SHA,
  and exact test count.
- Do not hand-edit `dev/diffs/`; regenerate it through the documented Spark-clone
  workflow.
- Tokio callbacks may move across threads. Do not rely on thread-local state, retain
  `JNIEnv`, or fetch driver/task context from worker callbacks. Capture required state
  at plan construction and use explicit owned/shared lifetimes.
- Never "fix" a lifetime bug with sleeps, swallowed exceptions, leaked singletons, or
  destructor suppression.

## Artifact layout

- `research/` — candidate evidence, reproduction reports, PARK/KILL memos.
- `specs/` — implementation or investigation specs approved for an agent.
- `drafts/` — owner-review packets and public text drafts; nothing here is public.
- `local-env.md` — untracked machine-specific handoff, paths, and environment state.
