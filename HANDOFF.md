# HANDOFF — Apache DataFusion Comet (Host 3060)

## Overview & Location
- Workstation Host: 3060 (Ubuntu Linux)
- Primary Code Checkout: /home/hsnl/oss/datafusion-comet
- Ops & Rules Directory: /home/hsnl/oss-comet-scan (also copied to /home/hsnl/oss/datafusion-comet/oss-comet-scan)

## Operational Environment
- Shell source environment before running Maven/Rust tests:
  source /home/hsnl/oss/datafusion-comet/.env.local
- Java: Java 17
- Scala: Scala 2.12
- Spark: Spark 3.5.8

## Recent Accomplishments
1. **Issue #5089** (Decimal Precision Validation): MERGED via PR #5160 (https://github.com/apache/datafusion-comet/pull/5160)
2. **Issue #5093** (Unary Negation ANSI Null Slot Overflow): OPEN / PUBLISHED via PR #5162 (https://github.com/apache/datafusion-comet/pull/5162)
   - Upstream PR: https://github.com/apache/datafusion-comet/pull/5162
   - Fork Branch: Smallfu666:internal/issue-5093-negative-null-overflow
   - Head Commit SHA: 9c09a80eab9c5f5070447cb6289cff0b63fc66a9

## Verification Commands
- Rust Format Check:
  cd /home/hsnl/oss/datafusion-comet/native && cargo fmt --check
- Rust Clippy Check:
  cd /home/hsnl/oss/datafusion-comet/native && cargo clippy -p datafusion-comet-spark-expr --all-targets -- -D warnings
- Rust Unit Tests:
  cd /home/hsnl/oss/datafusion-comet/native && cargo test -p datafusion-comet-spark-expr
- JVM Suite Test:
  cd /home/hsnl/oss/datafusion-comet && source .env.local && ./mvnw test -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.comet.exec.CometExecSuite

## Guidelines for 3060 Agent
- Read AGENTS.md and STATUS.md in oss-comet-scan/.
- Do not perform any public action (commenting, taking, opening PRs, pushing branches) without explicit Owner Review Gate approval.
- Maintain clean single-commit branches relative to upstream/main.
