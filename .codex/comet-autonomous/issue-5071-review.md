# Apache DataFusion Comet Issue #5071

## Classification
READY_FOR_OWNER_REVIEW

## Issue
- URL: https://github.com/apache/datafusion-comet/issues/5071
- Title: ANSI arithmetic overflow errors: wrong error class for Byte/Short, wrong type names, missing try_ suggestions
- Claim comment: https://github.com/apache/datafusion-comet/issues/5071#issuecomment-5141173145
- Current upstream status: OPEN

## Git
- Upstream baseline SHA: 8782d95331f5d6f4377a293d2c38094c41671e96
- Fork: Smallfu666/datafusion-comet
- Branch: internal/issue-5071-ansi-arithmetic-overflow-type-names
- Full head SHA: e1e7d739515de3719cd78446defbaefdabb8f159
- Commit title: fix: align ANSI arithmetic overflow error classes, type names, and suggestions with Spark (#5071)
- Compare link: https://github.com/apache/datafusion-comet/compare/main...Smallfu666:datafusion-comet:internal/issue-5071-ansi-arithmetic-overflow-type-names

## Collision Check
- Assignee: Smallfu666
- Competing issue comments: None
- Matching open PRs: None
- Related merged PRs: None
- Result: Clean ownership

## Root Cause
1. `checked_arithmetic.rs` hard-coded `SparkError::ArithmeticOverflow` with `from_type: "integer"` for all integer types including Byte, Short, and Long.
2. Spark 4.1 expects `BINARY_ARITHMETIC_OVERFLOW` with operand values and operator symbol for Byte/Short binary operations.
3. Long integer overflow produced "integer overflow" instead of "long overflow" in both `checked_arithmetic.rs` and `sum_int.rs`.
4. `abs.rs` passed internal Rust type names ("Int8", "Int16", "Int32", "Int64", "Decimal128", "Decimal256") instead of Spark type names ("byte", "short", "integer", "long", "decimal").
5. `negative.rs` scalar path passed `" caused"` as the type string due to a typo.
6. `ShimSparkErrorConverter.scala` passed empty string for alternative suggestion in `ArithmeticOverflow`.

## Fix
1. Updated `checked_arithmetic.rs` to construct `BinaryArithmeticOverflow` for Byte and Short types under ANSI mode carrying exact operand values, symbol, and suggestion function name.
2. Fixed type names for Int32 ("integer") and Int64 ("long") in `checked_arithmetic.rs` and `sum_int.rs`.
3. Replaced Rust type names in `abs.rs` with Spark standard type names ("byte", "short", "integer", "long", "decimal").
4. Corrected typo `" caused"` to `"byte"` and `"short"` in `negative.rs` scalar path.
5. Updated `SparkError::ArithmeticOverflow` to optional `function_name` field and updated `ShimSparkErrorConverter.scala` across Spark 3.4, 3.5, and 4.x shims to format ``Use `$fn` to tolerate overflow and return NULL instead.`` when `functionName` is provided.

## Semantic Review
- ANSI: Fully matches Spark error classes (`BINARY_ARITHMETIC_OVERFLOW` vs `ARITHMETIC_OVERFLOW`), type names, and suggestion messages.
- Legacy: Unchanged.
- Scalar: Unary minus and abs scalar paths now match array paths.
- Array: Array paths updated.
- Null: Preserves null semantics.
- Error mapping: Uses exact `BinaryArithmeticOverflow` and `ArithmeticOverflow` with proper parameters.
- Spark compatibility: Verified against Spark error class contracts.
- Arrow/DataFusion behavior: Native arithmetic kernel preserves Arrow validity buffers and zeroed null slots.

## Before/After Evidence
- Pre-fix: Byte/Short addition raised `[ARITHMETIC_OVERFLOW] integer overflow.`
- Post-fix: Byte/Short addition raises `[BINARY_ARITHMETIC_OVERFLOW] <val1> + <val2> caused overflow. Use `try_add` to tolerate overflow...`
- Pre-fix: Long addition raised `[ARITHMETIC_OVERFLOW] integer overflow.`
- Post-fix: Long addition raises `[ARITHMETIC_OVERFLOW] long overflow.`
- Pre-fix: Scalar unary minus on i8::MIN raised `[ARITHMETIC_OVERFLOW]  caused overflow.`
- Post-fix: Scalar unary minus on i8::MIN raises `[ARITHMETIC_OVERFLOW] byte overflow.`
- Pre-fix: `abs(-9223372036854775808L)` raised `[ARITHMETIC_OVERFLOW] Int64 overflow.`
- Post-fix: `abs(-9223372036854775808L)` raises `[ARITHMETIC_OVERFLOW] long overflow.`

## Tests Added
- `test_ansi_overflow_errors` in `checked_arithmetic.rs`: verifies `Int8` (Byte) and `Int16` (Short) raise `BinaryArithmeticOverflow` with correct formatting, `Int32` raises `integer overflow`, and `Int64` raises `long overflow`.

## Commands Executed
- `cargo fmt --check`: PASSED
- `cargo clippy -p datafusion-comet-spark-expr --all-targets -- -D warnings`: PASSED (0 warnings)
- `cargo test -p datafusion-comet-spark-expr`: PASSED (567 passed, 0 failed)
- `./mvnw test -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.comet.exec.CometExecSuite`: PASSED (141 succeeded, 0 failed)

## Diff
- Files: 9 files changed
- Additions: +162
- Deletions: -54
- Production-code scope: `native/common/src/error.rs`, `native/spark-expr/src/lib.rs`, `native/spark-expr/src/agg_funcs/sum_int.rs`, `native/spark-expr/src/math_funcs/abs.rs`, `native/spark-expr/src/math_funcs/checked_arithmetic.rs`, `native/spark-expr/src/math_funcs/negative.rs`, `spark/src/main/spark-3.*/org/apache/spark/sql/comet/shims/ShimSparkErrorConverter.scala`, `spark/src/main/spark-4.x/org/apache/spark/sql/comet/shims/ShimSparkErrorConverter.scala`
- Test-code scope: `checked_arithmetic.rs` tests module

## Risks
- Low risk. The change strictly improves error parity with Spark in ANSI mode and does not touch execution logic or legacy non-ANSI paths.

## Suggested PR
Title: fix: align ANSI arithmetic overflow error classes and type names with Spark (#5071)

Body:
Closes #5071

## Summary
For several arithmetic expressions under ANSI mode, Comet's error class, error message, or parameters differed from Spark:
1. `Add`/`Subtract`/`Multiply`/`Divide` on TINYINT/SMALLINT raised `ARITHMETIC_OVERFLOW` with `from_type: "integer"` instead of `BINARY_ARITHMETIC_OVERFLOW`.
2. Long integer overflow and `sum(int)` overflow reported `"integer overflow"` instead of `"long overflow"`.
3. `Abs` reported Rust type names (`"Int8"`, `"Int16"`, `"Int32"`, `"Int64"`, `"Decimal128"`, `"Decimal256"`) instead of Spark type names (`"byte"`, `"short"`, `"integer"`, `"long"`, `"decimal"`).
4. `UnaryMinus` scalar path for Byte/Short reported `" caused"` as the type string.
5. `ArithmeticOverflow` omitted `try_` suggestions.

This PR aligns all error classes, type names, operand formatting, and `try_` suggestions with Spark.

## Tests
- Added native Rust unit tests in `checked_arithmetic.rs` asserting exact error variants and strings for Byte, Short, Int, and Long overflow.
- Passed all native Rust unit tests (`cargo test -p datafusion-comet-spark-expr`).
- Passed JVM `CometExecSuite` under Spark 3.5 profile.

## Public Actions
- `take`: https://github.com/apache/datafusion-comet/issues/5071#issuecomment-5141173145
- fork push: https://github.com/Smallfu666/datafusion-comet/tree/internal/issue-5071-ansi-arithmetic-overflow-type-names
- upstream PR: NOT OPENED
- other comments: NONE
