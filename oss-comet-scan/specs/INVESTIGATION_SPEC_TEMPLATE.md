# Investigation spec: ISSUE — TITLE

## Pinned inputs

- Upstream base SHA:
- Candidate evidence:
- Implementer agent:
- Source worktree:

## Root-cause hypothesis

State one falsifiable primary hypothesis and the strongest competing explanation.

## Scope

### In scope

- _To be completed._

### Out of scope

- _To be completed._

## Acceptance criteria

- Pre-fix reproduction is preserved.
- Production change is minimal and addresses the observed mechanism.
- Regression test fails before and passes after.
- Spark/DataFusion semantics and native/fallback path are explicit.
- Negative controls pass.
- Exact commands, profiles, suite counts, and limitations are recorded.
- No research notes, raw logs, prompts, handoffs, or private paths enter the branch.

## Test matrix

| Dimension | Required values | Result |
|---|---|---|
| Spark profile |  |  |
| Java version |  |  |
| Rust build | debug/release as relevant |  |
| Native/fallback |  |  |
| Positive case |  |  |
| Negative control |  |  |

## Stop conditions

Stop and PARK if the issue no longer reproduces, an active duplicate appears, the fix
requires an unapproved redesign, or the environment cannot establish the required
oracle/path evidence.

## Independent review handoff

Provide base/head SHAs, changed files, test evidence, known failures, and remaining
risks. The reviewer must be a fresh agent, use a read-only code-review workflow, and
must not modify code.
