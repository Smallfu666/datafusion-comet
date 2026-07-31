# Comet issue research playbook

Use this playbook for reconnaissance only. It does not authorize public comments,
assignment, or PR creation.

## 1. Establish the base

Record:

- upstream repository and pinned `main` SHA;
- checked-at timestamp;
- local source/worktree status;
- relevant Spark, Java, Rust, Python, and native build profiles.

Do not fetch/reset a checkout owned by another active agent.

## 2. Read the complete issue state

Example queries:

```bash
gh issue view ISSUE --repo apache/datafusion-comet \
  --json number,title,state,author,createdAt,updatedAt,labels,assignees,body,comments,url

gh api repos/apache/datafusion-comet/issues/ISSUE/timeline \
  -H 'Accept: application/vnd.github+json'
```

Capture reporter environment, expected/actual behavior, reproduction assets, maintainer
signals, dependencies, and unresolved questions.

## 3. Duplicate and ownership scan

Search both issue number and mechanism keywords:

```bash
gh search prs --repo apache/datafusion-comet --state open 'ISSUE'
gh search prs --repo apache/datafusion-comet --merged 'ISSUE'
gh search prs --repo apache/datafusion-comet --state open \
  '"MECHANISM TERM" OR "ERROR SIGNATURE"'
gh search prs --repo apache/datafusion-comet --merged \
  '"MECHANISM TERM" OR "ERROR SIGNATURE"'
gh search commits --repo apache/datafusion-comet '"MECHANISM TERM"'
```

If the behavior belongs to DataFusion or `datafusion-spark`, repeat the mechanism search
in those repositories. Inspect linked branches and recent commits; zero issue comments
does not mean nobody is working on it.

## 4. Strategic and roadmap gate

Read the current Comet roadmap and recent maintainer activity. Write:

- why this issue;
- why now;
- why this owner/environment has an evidence advantage;
- expected patch size and verification cost;
- what would immediately PARK or KILL it.

Prefer correctness, crashes, bounded memory/lifetime failures, and measurable production
performance gaps over cosmetic or speculative work.

## 5. Reproduction contract

A PROMOTE candidate needs:

- current-main pre-fix failure;
- smallest deterministic input;
- vanilla Spark or another explicit oracle;
- proof of native execution, partial execution, or fallback;
- exact expected and actual result/exception/plan;
- exact suite and test count;
- at least one negative control;
- pinned artifacts and caveats.

For performance issues, separate compile/startup noise from steady-state execution and
record warmup, repetitions, data shape, partitions, memory, and confidence. Never claim
Comet/Gluten superiority from an uncontrolled run.

## 6. Decision

- **PROMOTE:** write an investigation spec and assign one implementer.
- **PARK:** record the blocking event and a concrete revisit condition.
- **KILL:** record the disproving evidence so future agents do not repeat the work.

After implementation, assign a fresh read-only reviewer and prepare an owner-review
packet. Publication remains owner-gated.
