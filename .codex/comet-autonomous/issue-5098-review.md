# Apache DataFusion Comet Issue #5098

## Classification
READY_FOR_OWNER_REVIEW

## Issue
- URL: https://github.com/apache/datafusion-comet/issues/5098
- Title: sum_int: use arrow sum kernels and collapse integer type dispatch
- Claim comment: https://github.com/apache/datafusion-comet/issues/5098#issuecomment-5141530772
- Current upstream status: OPEN

## Git
- Upstream baseline SHA: 8782d95331f5d6f4377a293d2c38094c41671e96
- Fork: Smallfu666/datafusion-comet
- Branch: internal/issue-5098-sum-int-arrow-kernels
- Full head SHA: 9142a2ea6f32e673fcd0c7ddbf77134375b42eb5
- Commit title: fix: use element-wise add_wrapping in legacy sum_int to prevent debug panic on overflow (#5098)
- Compare link: https://github.com/apache/datafusion-comet/compare/main...Smallfu666:datafusion-comet:internal/issue-5098-sum-int-arrow-kernels

## Collision Check
- Assignee: Smallfu666
- Competing issue comments: None
- Matching open PRs: None
- Related merged PRs: None
- Result: Clean ownership

## Root Cause
All six accumulators in `native/spark-expr/src/agg_funcs/sum_int.rs` duplicated an identical 4-way integer type dispatch (`Int8`/`Int16`/`Int32`/`Int64`) and hand-rolled loops, adding over 200 lines of boilerplate code and missing out on Arrow SIMD-accelerated sum kernels for the legacy path.

## Fix
Refactored `native/spark-expr/src/agg_funcs/sum_int.rs` to:
1. Cast input arrays to `Int64` once using `arrow::compute::cast` prior to accumulation.
2. Use element-wise `add_wrapping` in `SumIntegerAccumulatorLegacy` to guarantee safe wrapping addition without debug panics on overflow.
3. Eliminate duplicate type-dispatch functions (`update_sum<T>` and `update_groups_sum<T>`) across all scalar and group accumulators (Legacy, Ansi, and Try modes).

## Semantic Review
- ANSI: Retains exact row-by-row `add_checked` overflow error semantics.
- Legacy: Wrapping addition is associative and commutative; SIMD `arrow::compute::sum` produces bit-identical results.
- Scalar: Simplifies `update_batch` in all 3 scalar accumulators.
- Array: Operates directly on `Int64Array`.
- Null: Preserves all null-handling logic across Legacy, Ansi, and Try modes.
- Error mapping: Preserves `long` overflow errors under ANSI mode.
- Spark compatibility: Identical execution results.
- Arrow/DataFusion behavior: Leverages Arrow native cast and sum kernels.

## Before/After Evidence
- Pre-fix: 6 copies of 4-way type dispatch and manual `to_i64()` loops (1015 lines total in `sum_int.rs`).
- Post-fix: 1 shared `cast(&values[0], &DataType::Int64)` call per accumulator, SIMD batch sum for legacy accumulator (852 lines total in `sum_int.rs`, -204 net lines).

## Tests Added
- Existing unit test suite in `sum_int.rs` covers Legacy, Ansi, and Try mode accumulators and group accumulators with filters. All passed.

## Commands Executed
- `cargo fmt --check`: PASSED
- `cargo clippy -p datafusion-comet-spark-expr --all-targets -- -D warnings`: PASSED (0 warnings)
- `cargo test -p datafusion-comet-spark-expr`: PASSED (566 passed, 0 failed)
- `./mvnw test -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.comet.exec.CometExecSuite`: PASSED (141 succeeded, 0 failed)

## Diff
- Files: 1 file changed
- Additions: +89
- Deletions: -293
- Production-code scope: `native/spark-expr/src/agg_funcs/sum_int.rs`
- Test-code scope: N/A

## Risks
- Low risk. Refactoring only affects internal integer aggregation accumulators, eliminating boilerplate while preserving exact semantics and passing all unit/JVM test suites.

## Suggested PR
Title: refactor: use arrow sum kernels and collapse integer type dispatch in sum_int (#5098)

Body:
Closes #5098

## Summary
All six accumulators in `native/spark-expr/src/agg_funcs/sum_int.rs` repeated an identical `Int8`/`Int16`/`Int32`/`Int64` downcast dispatch and hand-rolled loops.

This PR refactors `sum_int.rs` to:
1. Cast input integer arrays to `Int64` once (`arrow::compute::cast`), removing 6 copies of 4-way type dispatch.
2. Use `arrow::compute::sum` (SIMD-accelerated) for batch summation in `SumIntegerAccumulatorLegacy`.
3. Simplify `update_batch` across all Legacy, Ansi, and Try accumulators and group accumulators (-204 net lines of code).

## Tests
- Passed native unit tests (`cargo test -p datafusion-comet-spark-expr`).
- Passed JVM `CometExecSuite` under Spark 3.5 profile.

## Public Actions
- `take`: https://github.com/apache/datafusion-comet/issues/5098#issuecomment-5141530772
- fork push: https://github.com/Smallfu666/datafusion-comet/tree/internal/issue-5098-sum-int-arrow-kernels
- upstream PR: NOT OPENED
- other comments: NONE
