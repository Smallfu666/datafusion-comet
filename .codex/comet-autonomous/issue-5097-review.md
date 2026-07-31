# Apache DataFusion Comet Issue #5097

## Classification
READY_FOR_OWNER_REVIEW

## Issue
- URL: https://github.com/apache/datafusion-comet/issues/5097
- Title: cast_map_to_map drops entries null buffer and ignores target sorted flag
- Claim comment: https://github.com/apache/datafusion-comet/issues/5097#issuecomment-5141486161
- Current upstream status: OPEN

## Git
- Upstream baseline SHA: 8782d95331f5d6f4377a293d2c38094c41671e96
- Fork: Smallfu666/datafusion-comet
- Branch: internal/issue-5097-cast-map-to-map
- Full head SHA: 37373e9fd1c1eb8c9d06854bcbc5caeb942f6d2b
- Commit title: refactor: preserve field metadata with Arc::clone for struct entry fields in cast_map_to_map (#5097)
- Compare link: https://github.com/apache/datafusion-comet/compare/main...Smallfu666:datafusion-comet:internal/issue-5097-cast-map-to-map

## Collision Check
- Assignee: Smallfu666
- Competing issue comments: None
- Matching open PRs: None
- Related merged PRs: None
- Result: Clean ownership

## Root Cause
`cast_map_to_map` in `native/spark-expr/src/conversion_funcs/cast.rs` manual MapArray construction passed `None` for `StructArray` entries null buffer, dropping `map_array.entries().nulls()`, and passed `*from_sorted` instead of `*to_sorted` for target MapArray sorted metadata.

## Fix
Updated `cast_map_to_map` in `cast.rs` to:
1. Pass `map_array.entries().nulls().cloned()` when constructing `entries_struct` StructArray.
2. Use `*to_sorted` when constructing target `MapArray`.

## Semantic Review
- ANSI: N/A
- Legacy: Preserves existing structure and nullability.
- Scalar: N/A
- Array/Map: Correctly preserves entries null buffer and applies target map metadata.
- Null: Preserves both top-level map nulls and entries struct nulls.
- Error mapping: Standard Spark cast options.
- Spark compatibility: Map data structures preserve field names and metadata.
- Arrow/DataFusion behavior: Follows Arrow MapArray specifications.

## Before/After Evidence
- Pre-fix: `cast_map_to_map` created entries struct with `None` null buffer and ignored target `to_sorted` metadata.
- Post-fix: `cast_map_to_map` preserves entries null buffer and uses target `to_sorted` metadata.

## Tests Added
- `test_cast_map_to_map_preserves_entries_nulls_and_sorted_flag` in `cast.rs`: verifies MapArray casting preserves entries null buffer and sets target sorted metadata.

## Commands Executed
- `cargo fmt --check`: PASSED
- `cargo clippy -p datafusion-comet-spark-expr --all-targets -- -D warnings`: PASSED (0 warnings)
- `cargo test -p datafusion-comet-spark-expr`: PASSED (567 passed, 0 failed)
- `./mvnw test -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.comet.CometCastSuite`: PASSED (162 succeeded, 0 failed)

## Diff
- Files: 1 file changed
- Additions: +56
- Deletions: -5
- Production-code scope: `native/spark-expr/src/conversion_funcs/cast.rs`
- Test-code scope: `cast.rs` tests module

## Risks
- Low risk. Strictly fixes metadata propagation and entries struct null buffer preservation during MapArray casting.

## Suggested PR
Title: fix: preserve entries nulls and target sorted flag in cast_map_to_map (#5097)

Body:
Closes #5097

## Summary
`cast_map_to_map` in `native/spark-expr/src/conversion_funcs/cast.rs` manual MapArray construction passed `None` for the entries struct null buffer, dropping `map_array.entries().nulls()`, and passed `*from_sorted` instead of target `*to_sorted`.

This PR preserves `map_array.entries().nulls()` and uses target `*to_sorted` when constructing the target MapArray.

## Tests
- Added native Rust unit test `test_cast_map_to_map_preserves_entries_nulls_and_sorted_flag` in `cast.rs`.
- Passed native unit tests (`cargo test -p datafusion-comet-spark-expr`).
- Passed JVM `CometCastSuite` under Spark 3.5 profile.

## Public Actions
- `take`: https://github.com/apache/datafusion-comet/issues/5097#issuecomment-5141486161
- fork push: https://github.com/Smallfu666/datafusion-comet/tree/internal/issue-5097-cast-map-to-map
- upstream PR: NOT OPENED
- other comments: NONE
