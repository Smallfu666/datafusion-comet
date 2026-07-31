# STATUS — oss-comet-scan

_Last updated: 2026-07-31 — Active Upstream Contribution_

## Current state

- Target: `apache/datafusion-comet`, default branch `main`.
- Lane state: **ACTIVE UPSTREAM CONTRIBUTIONS**.
- Upstream baseline SHA: `8782d95331f5d6f4377a293d2c38094c41671e96`.
- Host 3060 workstation environment: `/home/hsnl/oss/datafusion-comet`.

## Active & Completed Issues

1. **Issue #5089** (Refactor decimal precision validation):
   - Status: **MERGED**
   - Upstream PR: [#5160](https://github.com/apache/datafusion-comet/pull/5160)

2. **Issue #5093** (Fix unary negation ANSI null-slot overflow check):
   - Status: **OPEN / PUBLISHED**
   - Upstream PR: [#5162](https://github.com/apache/datafusion-comet/pull/5162)
   - Fork Branch: `Smallfu666:internal/issue-5093-negative-null-overflow`
   - Head Commit SHA: `9c09a80eab9c5f5070447cb6289cff0b63fc66a9`
   - Verification: Passed 577 Rust unit tests, cargo fmt, clippy (0 warnings), and 141 JVM `CometExecSuite` tests.

## Verified upstream orientation

- Comet accelerates Spark with Arrow-native data and Rust/DataFusion execution.
- Supported Spark lines currently include 3.4, 3.5, 4.0, and 4.1.
- Contributor workflow: Maintain clean single-commit PRs, run Rust fmt/clippy and JVM suites before pushing.

## Host 3060 Environment Facts

- Source checkout path: `/home/hsnl/oss/datafusion-comet`
- Environment config: `/home/hsnl/oss/datafusion-comet/.env.local`
- Java: 17
- Scala: 2.12
- Spark: 3.5.8
- Rust verification: `cd native && cargo fmt --check && cargo clippy -p datafusion-comet-spark-expr --all-targets -- -D warnings && cargo test -p datafusion-comet-spark-expr`
- JVM verification: `./mvnw test -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.comet.exec.CometExecSuite`

