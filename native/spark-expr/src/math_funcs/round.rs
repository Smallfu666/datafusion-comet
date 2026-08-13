// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::math_funcs::utils::{get_precision_scale, make_decimal_array, make_decimal_scalar};
use crate::SparkError;
use arrow::array::{Array, ArrowNativeTypeOp};
use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{exec_err, internal_err, DataFusionError, ScalarValue};
use datafusion::functions::math::round::RoundFunc;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_plan::ColumnarValue;
use std::{cmp::min, sync::Arc};

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr, $FAIL_ON_ERROR:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            if $FAIL_ON_ERROR {
                ($X - rem)
                    .sub_checked($DIV)
                    .map_err(|_| ArrowError::ArithmeticOverflow("Overflow".to_string()))
            } else {
                Ok(($X - rem).sub_wrapping($DIV))
            }
        } else if rem >= $HALF {
            if $FAIL_ON_ERROR {
                ($X - rem)
                    .add_checked($DIV)
                    .map_err(|_| ArrowError::ArithmeticOverflow("Overflow".to_string()))
            } else {
                Ok(($X - rem).add_wrapping($DIV))
            }
        } else {
            if $FAIL_ON_ERROR {
                $X.sub_checked(rem)
                    .map_err(|_| ArrowError::ArithmeticOverflow("Overflow".to_string()))
            } else {
                Ok($X.sub_wrapping(rem))
            }
        }
    }};
}

// Round a single native integer when `10^(-point)` does not fit in the native
// integer type but still fits in i128. The caller has already excluded the
// case where `div` fits the native type, so `|x| <= NATIVE::MAX < div`, which
// makes `x % div == x`: no division is needed here, and the result is either
// `0` (when `|x| < half`) or `sign(x) * div` — the latter always overflows the
// native type. Under ANSI we throw, under legacy we wrap by truncation
// (matches `BigDecimal.longValue`'s low-64-bit semantics).
macro_rules! integer_round_widened {
    ($X:expr, $DIV:expr, $HALF:expr, $NATIVE:ty, $FAIL_ON_ERROR:expr) => {{
        let x128 = $X as i128;
        debug_assert!(
            x128 > -$DIV && x128 < $DIV,
            "integer_round_widened! requires div to overflow the native type"
        );
        if x128 > -$HALF && x128 < $HALF {
            Ok(0 as $NATIVE)
        } else if $FAIL_ON_ERROR {
            Err(ArrowError::ArithmeticOverflow("Overflow".to_string()))
        } else {
            Ok((if x128 >= $HALF { $DIV } else { -$DIV }) as $NATIVE)
        }
    }};
}

/// Lift an error out of the rounding kernels. `arity::try_unary` can only carry an `ArrowError`,
/// so an ANSI overflow travels as `ArrowError::ArithmeticOverflow` and is rebuilt here as a
/// `SparkError`, which is what carries the `ARITHMETIC_OVERFLOW` class and its parameters across
/// the JNI boundary. Any other Arrow failure is passed through unchanged.
fn lift_round_error(err: ArrowError) -> DataFusionError {
    match err {
        ArrowError::ArithmeticOverflow(_) => SparkError::RoundOverflow.into(),
        other => {
            DataFusionError::ArrowError(Box::from(other), Some(DataFusionError::get_back_trace()))
        }
    }
}

// Unwrap a rounding result on the scalar path, returning early from the enclosing function on
// error. Used by `round_integer_scalar!`, which has no kernel to propagate through.
macro_rules! round_scalar_result {
    ($RESULT:expr) => {
        match $RESULT {
            Ok(v) => Some(v),
            Err(e) => return Err(lift_round_error(e)),
        }
    };
}

macro_rules! round_integer_array {
    ($ARRAY:expr, $POINT:expr, $TYPE:ty, $NATIVE:ty, $FAIL_ON_ERROR:expr) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let ten: $NATIVE = 10;
        let point_abs = (-(*$POINT)) as u32;
        let result: $TYPE = if let Some(div) = ten.checked_pow(point_abs) {
            let half = div / 2;
            arrow::compute::kernels::arity::try_unary(array, |x| {
                integer_round!(x, div, half, $FAIL_ON_ERROR)
            })
            .map_err(lift_round_error)?
        } else if let Some(div) = 10_i128.checked_pow(point_abs) {
            let half = div / 2;
            arrow::compute::kernels::arity::try_unary(array, |x| {
                integer_round_widened!(x, div, half, $NATIVE, $FAIL_ON_ERROR)
            })
            .map_err(lift_round_error)?
        } else {
            // Even i128 cannot hold 10^(-point); every bounded native
            // integer rounds to 0.
            arrow::compute::kernels::arity::try_unary(array, |_| Ok(0)).map_err(lift_round_error)?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($SCALAR:expr, $POINT:expr, $TYPE:expr, $NATIVE:ty, $FAIL_ON_ERROR:expr) => {{
        let ten: $NATIVE = 10;
        let point_abs = (-(*$POINT)) as u32;
        let scalar_opt = match $SCALAR {
            None => None,
            Some(x) => {
                if let Some(div) = ten.checked_pow(point_abs) {
                    let half = div / 2;
                    round_scalar_result!(integer_round!(*x, div, half, $FAIL_ON_ERROR))
                } else if let Some(div) = 10_i128.checked_pow(point_abs) {
                    let half = div / 2;
                    round_scalar_result!(integer_round_widened!(
                        *x,
                        div,
                        half,
                        $NATIVE,
                        $FAIL_ON_ERROR
                    ))
                } else {
                    // Even i128 cannot hold 10^(-point); every bounded native
                    // integer rounds to 0.
                    Some(0)
                }
            }
        };
        Ok(ColumnarValue::Scalar($TYPE(scalar_opt)))
    }};
}

/// `round` function that simulates Spark `round` expression
pub fn spark_round(
    args: &[ColumnarValue],
    data_type: &DataType,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    let point = &args[1];
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(point))) = point else {
        return internal_err!("Invalid point argument for Round(): {:#?}", point);
    };
    // DataFusion's RoundFunc expects Int32 for decimal_places
    let point_i32 = ColumnarValue::Scalar(ScalarValue::Int32(Some(*point as i32)));
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 if *point < 0 => {
                round_integer_array!(array, point, Int64Array, i64, fail_on_error)
            }
            DataType::Int32 if *point < 0 => {
                round_integer_array!(array, point, Int32Array, i32, fail_on_error)
            }
            DataType::Int16 if *point < 0 => {
                round_integer_array!(array, point, Int16Array, i16, fail_on_error)
            }
            DataType::Int8 if *point < 0 => {
                round_integer_array!(array, point, Int8Array, i8, fail_on_error)
            }
            DataType::Decimal128(_, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            DataType::Float32 | DataType::Float64 => {
                let round_udf = RoundFunc::new();
                let return_field = Arc::new(Field::new("round", array.data_type().clone(), true));
                let args_for_round = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::clone(array)), point_i32.clone()],
                    number_rows: array.len(),
                    return_field,
                    arg_fields: vec![],
                    config_options: Arc::new(ConfigOptions::default()),
                };
                round_udf.invoke_with_args(args_for_round)
            }
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int64(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int64, i64, fail_on_error)
            }
            ScalarValue::Int32(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int32, i32, fail_on_error)
            }
            ScalarValue::Int16(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int16, i16, fail_on_error)
            }
            ScalarValue::Int8(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int8, i8, fail_on_error)
            }
            ScalarValue::Decimal128(a, _, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            ScalarValue::Float32(_) | ScalarValue::Float64(_) => {
                let round_udf = RoundFunc::new();
                let data_type = a.data_type();
                let return_field = Arc::new(Field::new("round", data_type, true));
                let args_for_round = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Scalar(a.clone()), point_i32.clone()],
                    number_rows: 1,
                    return_field,
                    arg_fields: vec![],
                    config_options: Arc::new(ConfigOptions::default()),
                };
                round_udf.invoke_with_args(args_for_round)
            }
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
    }
}

// Spark uses BigDecimal. See RoundBase implementation in Spark. Instead, we do the same by
// 1) add the half of divisor, 2) round down by division, 3) adjust precision by multiplication
#[inline]
fn decimal_round_f(scale: &i8, point: &i64) -> Box<dyn Fn(i128) -> i128> {
    if *point < 0 {
        if let Some(div) = 10_i128.checked_pow((-(*point) as u32) + (*scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow_wrapping((-(*point)) as u32);
            // i128 can hold 39 digits of a base 10 number, adding half will not cause overflow
            Box::new(move |x: i128| (x + x.signum() * half) / div * mul)
        } else {
            Box::new(move |_: i128| 0)
        }
    } else {
        let div = 10_i128.pow_wrapping((*scale as u32) - min(*scale as u32, *point as u32));
        let half = div / 2;
        Box::new(move |x: i128| (x + x.signum() * half) / div)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{spark_round, SparkError};

    use arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    };
    use arrow::datatypes::DataType;
    use datafusion::common::cast::{as_float32_array, as_float64_array, as_int64_array};
    use datafusion::common::{DataFusionError, Result, ScalarValue};
    use datafusion::physical_plan::ColumnarValue;

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f32_array() -> Result<()> {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                125.2345, 15.3455, 0.1234, 0.125, 0.785, 123.123,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Array(result) = spark_round(&args, &DataType::Float32, false)? else {
            unreachable!()
        };
        let floats = as_float32_array(&result)?;
        let expected = Float32Array::from(vec![125.23, 15.35, 0.12, 0.13, 0.79, 123.12]);
        assert_eq!(floats, &expected);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f64_array() -> Result<()> {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                125.2345, 15.3455, 0.1234, 0.125, 0.785, 123.123,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Array(result) = spark_round(&args, &DataType::Float64, false)? else {
            unreachable!()
        };
        let floats = as_float64_array(&result)?;
        let expected = Float64Array::from(vec![125.23, 15.35, 0.12, 0.13, 0.79, 123.12]);
        assert_eq!(floats, &expected);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f32_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(125.2345))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Float32(Some(result))) =
            spark_round(&args, &DataType::Float32, false)?
        else {
            unreachable!()
        };
        assert_eq!(result, 125.23);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f64_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(125.2345))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(result))) =
            spark_round(&args, &DataType::Float64, false)?
        else {
            unreachable!()
        };
        assert_eq!(result, 125.23);
        Ok(())
    }

    // Regression tests for https://github.com/apache/datafusion-comet/issues/5070:
    // round(Int64, scale) where `10^(-scale)` does not fit in i64. For scale=-19,
    // values with |x| >= 5e18 round to sign(x)*1e19, which does not fit in a long:
    // Spark throws under ANSI and wraps (low-order 64 bits) under legacy.

    // 1e19 truncated to the low 64 bits, matching `BigDecimal.longValue`. Note
    // this value is *negative* (-8446744073709551616): 1e19 exceeds i64::MAX, so
    // reinterpreting its low 64 bits as a signed long flips the sign. Rounding
    // -5e18 down to -1e19 therefore wraps to a positive value.
    const WRAPPED_1E19: i64 = 10_000_000_000_000_000_000u64 as i64;
    const WRAPPED_MINUS_1E19: i64 = -WRAPPED_1E19;

    /// Spark renders `ARITHMETIC_OVERFLOW` from `RoundBase` as the bare word `Overflow`, so the
    /// native side has to hand the JVM a `SparkError` rather than a stringified Arrow error.
    fn assert_round_overflow(err: DataFusionError) {
        let DataFusionError::External(ref e) = err else {
            panic!("expected DataFusionError::External carrying a SparkError, got: {err:?}");
        };
        match e.downcast_ref::<SparkError>() {
            Some(SparkError::RoundOverflow) => {}
            other => panic!("expected SparkError::RoundOverflow, got: {other:?}"),
        }
    }

    fn assert_round_int64_ansi_overflows(value: ColumnarValue) {
        let args = vec![value, ColumnarValue::Scalar(ScalarValue::Int64(Some(-19)))];
        assert_round_overflow(spark_round(&args, &DataType::Int64, true).unwrap_err());
    }

    /// The four integer widths at their largest value that rounds up out of range, on the
    /// `integer_round!` path (`10^(-scale)` still fits the native type). This is the common
    /// case; `assert_round_int64_ansi_overflows` covers the widened path.
    #[test]
    fn test_round_ansi_overflow_is_spark_error_for_all_int_widths() {
        let arrays: Vec<(ArrayRef, DataType)> = vec![
            (Arc::new(Int8Array::from(vec![125i8])), DataType::Int8),
            (Arc::new(Int16Array::from(vec![32765i16])), DataType::Int16),
            (
                Arc::new(Int32Array::from(vec![2147483645i32])),
                DataType::Int32,
            ),
            (
                Arc::new(Int64Array::from(vec![9223372036854775805i64])),
                DataType::Int64,
            ),
        ];
        for (array, data_type) in arrays {
            let args = vec![
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ];
            assert_round_overflow(spark_round(&args, &data_type, true).unwrap_err());
        }

        for (scalar, data_type) in [
            (ScalarValue::Int8(Some(125)), DataType::Int8),
            (ScalarValue::Int16(Some(32765)), DataType::Int16),
            (ScalarValue::Int32(Some(2147483645)), DataType::Int32),
            (
                ScalarValue::Int64(Some(9223372036854775805)),
                DataType::Int64,
            ),
        ] {
            let args = vec![
                ColumnarValue::Scalar(scalar),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ];
            assert_round_overflow(spark_round(&args, &data_type, true).unwrap_err());
        }
    }

    /// One below each value above still rounds down and succeeds, so the guard is not
    /// over-broad.
    #[test]
    fn test_round_ansi_just_inside_boundary_succeeds() -> Result<()> {
        for (scalar, data_type, expected) in [
            (
                ScalarValue::Int8(Some(124)),
                DataType::Int8,
                ScalarValue::Int8(Some(120)),
            ),
            (
                ScalarValue::Int16(Some(32764)),
                DataType::Int16,
                ScalarValue::Int16(Some(32760)),
            ),
            (
                ScalarValue::Int32(Some(2147483644)),
                DataType::Int32,
                ScalarValue::Int32(Some(2147483640)),
            ),
            (
                ScalarValue::Int64(Some(9223372036854775804)),
                DataType::Int64,
                ScalarValue::Int64(Some(9223372036854775800)),
            ),
        ] {
            let args = vec![
                ColumnarValue::Scalar(scalar),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ];
            let ColumnarValue::Scalar(result) = spark_round(&args, &data_type, true)? else {
                unreachable!()
            };
            assert_eq!(result, expected);
        }
        Ok(())
    }

    #[test]
    fn test_round_int64_negative_scale_overflow_ansi() {
        // ±5e18 rounds away from zero to ±1e19, which overflows i64.
        for value in [5_000_000_000_000_000_000i64, -5_000_000_000_000_000_000i64] {
            assert_round_int64_ansi_overflows(ColumnarValue::Array(Arc::new(Int64Array::from(
                vec![value],
            ))));
        }
    }

    #[test]
    fn test_round_int64_negative_scale_overflow_ansi_scalar() {
        for value in [5_000_000_000_000_000_000i64, -5_000_000_000_000_000_000i64] {
            assert_round_int64_ansi_overflows(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                value,
            ))));
        }
    }

    #[test]
    fn test_round_int64_negative_scale_overflow_legacy() -> Result<()> {
        // Under legacy mode, ±1e19 wraps to its low-order 64 bits.
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                5_000_000_000_000_000_000i64,
                -5_000_000_000_000_000_000i64,
                4_999_999_999_999_999_999i64,
                0i64,
                i64::MAX,
                i64::MIN,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(-19))),
        ];
        let ColumnarValue::Array(result) = spark_round(&args, &DataType::Int64, false)? else {
            unreachable!()
        };
        let longs = as_int64_array(&result)?;
        let expected = Int64Array::from(vec![
            WRAPPED_1E19,
            WRAPPED_MINUS_1E19,
            0i64,
            0i64,
            WRAPPED_1E19,
            WRAPPED_MINUS_1E19,
        ]);
        assert_eq!(longs, &expected);
        Ok(())
    }

    #[test]
    fn test_round_int64_negative_scale_below_threshold() -> Result<()> {
        // scale=-20: threshold is 5e19, which exceeds i64::MAX, so every long
        // rounds to 0 under both ANSI and legacy.
        let arr = Int64Array::from(vec![i64::MAX, i64::MIN, 0, 1_000_000_000_000_000_000]);
        for fail_on_error in [false, true] {
            let args = vec![
                ColumnarValue::Array(Arc::new(arr.clone())),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-20))),
            ];
            let ColumnarValue::Array(result) = spark_round(&args, &DataType::Int64, fail_on_error)?
            else {
                unreachable!()
            };
            let longs = as_int64_array(&result)?;
            assert_eq!(longs, &Int64Array::from(vec![0i64; arr.len()]));
        }
        Ok(())
    }

    #[test]
    fn test_round_int64_scale_below_i128_range() -> Result<()> {
        // scale=-40: 10^40 does not fit in i128 either, so `checked_pow` returns
        // None and the fallback returns 0 for every long, under both modes.
        let arr = Int64Array::from(vec![i64::MAX, i64::MIN, 0, 5_000_000_000_000_000_000]);
        for fail_on_error in [false, true] {
            let args = vec![
                ColumnarValue::Array(Arc::new(arr.clone())),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-40))),
            ];
            let ColumnarValue::Array(result) = spark_round(&args, &DataType::Int64, fail_on_error)?
            else {
                unreachable!()
            };
            let longs = as_int64_array(&result)?;
            assert_eq!(longs, &Int64Array::from(vec![0i64; arr.len()]));

            let scalar_args = vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5_000_000_000_000_000_000i64))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-40))),
            ];
            let ColumnarValue::Scalar(ScalarValue::Int64(result)) =
                spark_round(&scalar_args, &DataType::Int64, fail_on_error)?
            else {
                unreachable!()
            };
            assert_eq!(result, Some(0));
        }
        Ok(())
    }

    #[test]
    fn test_round_int64_negative_scale_null_scalar() -> Result<()> {
        // A null long stays null in every scale band: 10^(-scale) fits in i64
        // (-9), only in i128 (-19), or in neither (-40).
        for point in [-9i64, -19, -40] {
            for fail_on_error in [false, true] {
                let args = vec![
                    ColumnarValue::Scalar(ScalarValue::Int64(None)),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(point))),
                ];
                let ColumnarValue::Scalar(ScalarValue::Int64(result)) =
                    spark_round(&args, &DataType::Int64, fail_on_error)?
                else {
                    unreachable!()
                };
                assert_eq!(result, None, "scale={point}, ansi={fail_on_error}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_round_int64_negative_scale_legacy_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5_000_000_000_000_000_000i64))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(-19))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_round(&args, &DataType::Int64, false)?
        else {
            unreachable!()
        };
        assert_eq!(result, WRAPPED_1E19);
        Ok(())
    }
}
