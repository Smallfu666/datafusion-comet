# oss-comet-scan

Private research and coordination lane for Apache DataFusion Comet contributions.

## Architecture

```text
Smallfu666/oss-ops (private)
└── oss-comet-scan/             canonical research lane

projects/oss-comet-scan         convenient symlink to this directory
3060 workstation checkout      separate Comet source/build/test environment
Smallfu666/datafusion-comet     contribution fork, when needed
apache/datafusion-comet        upstream
```

Keeping source and research separate prevents private notes, agent prompts, raw logs,
and machine details from entering Apache contribution branches.

## Start here

1. Read `AGENTS.md`, `STATUS.md`, and both shared-policy symlinks.
2. Complete the 3060 environment handoff in untracked `local-env.md`.
3. Use `ISSUE_RESEARCH_PLAYBOOK.md` to scan and rank candidates.
4. Create one evidence file from `research/CANDIDATE_TEMPLATE.md`.
5. PROMOTE, PARK, or KILL the candidate before assigning implementation.
6. Use `specs/INVESTIGATION_SPEC_TEMPLATE.md` for implementation/investigation.
7. Return a completed `drafts/OWNER_REVIEW_TEMPLATE.md` before any public action.

No issue is claimed and no public Comet action is authorized by creating this lane.
