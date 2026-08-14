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

use crate::arithmetic_overflow_error;
use crate::SparkError;
use arrow::array::RecordBatch;
use arrow::compute::kernels::numeric::{neg, neg_wrapping};
use arrow::datatypes::IntervalDayTimeType;
use arrow::datatypes::{DataType, IntervalUnit, Schema};
use arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::{
    logical_expr::{interval_arithmetic::Interval, ColumnarValue},
    physical_expr::PhysicalExpr,
};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

pub fn create_negate_expr(
    expr: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    Ok(Arc::new(NegativeExpr::new(expr, fail_on_error)))
}

/// Negative expression
#[derive(Debug, Eq)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
}

impl Hash for NegativeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for NegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, fail_on_error: bool) -> Self {
        Self { arg, fail_on_error }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }

    /// Whether negating every value `range` can hold is strictly decreasing.
    ///
    /// Two's-complement negation maps the minimum of an integer type onto itself, so it
    /// is strictly decreasing only over a range that stays above that minimum. `evaluate`
    /// reaches it through `neg_wrapping` for every type in legacy mode, and in ANSI mode
    /// for every type it does not route to the overflow-checked `neg` -- which is the
    /// unsigned integers, since a signed one raises an overflow error there instead.
    fn negation_is_strictly_decreasing(&self, range: &Interval) -> bool {
        let data_type = range.data_type();
        if self.fail_on_error && !data_type.is_unsigned_integer() {
            return true;
        }
        match wrapping_order_boundary(&data_type) {
            // A null lower bound is unbounded below, so it holds the boundary.
            Some(boundary) => !range.lower().is_null() && range.lower() > &boundary,
            // `ExprProperties::new_unknown` reports a `Null` range for an expression of
            // any type, so a `Null` range is no evidence that this one cannot wrap.
            None => data_type != DataType::Null,
        }
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

/// Wrapping negation is monotone only above this boundary: the minimum for signed
/// integers and zero for unsigned integers. `None` for every other type, which Arrow's
/// `neg_wrapping` leaves to the overflow-checked `neg`.
fn wrapping_order_boundary(data_type: &DataType) -> Option<ScalarValue> {
    Some(match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(i8::MIN)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::MIN)),
        DataType::Int32 => ScalarValue::Int32(Some(i32::MIN)),
        DataType::Int64 => ScalarValue::Int64(Some(i64::MIN)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        _ => return None,
    })
}

fn map_neg_error(err: ArrowError, from_type: &'static str) -> DataFusionError {
    match err {
        ArrowError::ArithmeticOverflow(_) => arithmetic_overflow_error(from_type).into(),
        other => DataFusionError::from(other),
    }
}

impl PhysicalExpr for NegativeExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;

        // Overflow checks only apply in ANSI mode, and only the types listed in the
        // match below have a Spark overflow message. Everything else (float, decimal,
        // `Interval(MonthDayNano)`, ...) falls through to `neg_wrapping`.
        match arg {
            ColumnarValue::Array(array) => {
                if !self.fail_on_error {
                    return Ok(ColumnarValue::Array(neg_wrapping(array.as_ref())?));
                }
                // The shims render this as `{from_type} overflow` under
                // `ARITHMETIC_OVERFLOW`. For byte/short that is byte-identical to Spark
                // 4.x, which routes them through `MathUtils.negateExact` ("byte overflow" /
                // "short overflow"). Spark 3.4/3.5 instead throw
                // `_LEGACY_ERROR_TEMP_2043` ("- <sqlValue> caused overflow."); that error
                // class is out of reach here, so no string can match every version.
                let from_type = match array.data_type() {
                    DataType::Int8 => "byte",
                    DataType::Int16 => "short",
                    DataType::Int32 => "integer",
                    DataType::Int64 => "long",
                    // `neg` checks each `DayTime` component, so either `days` or `ms` at
                    // `i32::MIN` overflows; routing intervals here maps the Arrow overflow
                    // error onto Spark's, matching the scalar path below.
                    DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime) => {
                        "interval"
                    }
                    // Everything else falls through to `neg_wrapping`. Note it wraps only
                    // for integers: for any other type, `Interval(MonthDayNano)` included,
                    // it delegates to `neg`, so overflow is still detected -- it just
                    // surfaces as an Arrow error rather than a Spark one.
                    _ => return Ok(ColumnarValue::Array(neg_wrapping(array.as_ref())?)),
                };
                Ok(ColumnarValue::Array(
                    neg(array.as_ref()).map_err(|e| map_neg_error(e, from_type))?,
                ))
            }
            ColumnarValue::Scalar(scalar) => {
                if self.fail_on_error {
                    match scalar {
                        // Keep scalar overflow type names aligned with the array path.
                        ScalarValue::Int8(Some(i8::MIN)) => {
                            return Err(arithmetic_overflow_error("byte").into());
                        }
                        ScalarValue::Int16(Some(i16::MIN)) => {
                            return Err(arithmetic_overflow_error("short").into());
                        }
                        ScalarValue::Int32(Some(i32::MIN)) => {
                            return Err(arithmetic_overflow_error("integer").into());
                        }
                        ScalarValue::Int64(Some(i64::MIN)) => {
                            return Err(arithmetic_overflow_error("long").into());
                        }
                        ScalarValue::IntervalDayTime(value) => {
                            let (days, ms) =
                                IntervalDayTimeType::to_parts(value.unwrap_or_default());
                            if days == i32::MIN || ms == i32::MIN {
                                return Err(arithmetic_overflow_error("interval").into());
                            }
                        }
                        ScalarValue::IntervalYearMonth(Some(i32::MIN)) => {
                            return Err(arithmetic_overflow_error("interval").into());
                        }
                        ScalarValue::IntervalYearMonth(_) => {}
                        _ => {
                            // Overflow checks are not supported for other datatypes
                        }
                    }
                }
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NegativeExpr::new(
            Arc::clone(&children[0]),
            self.fail_on_error,
        )))
    }

    /// Given the child interval of a NegativeExpr, it calculates the NegativeExpr's interval.
    /// It replaces the upper and lower bounds after multiplying them with -1.
    /// Ex: `(a, b]` => `[-b, -a)`
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Interval::try_new(
            children[0].upper().arithmetic_negate()?,
            children[0].lower().arithmetic_negate()?,
        )
    }

    /// Returns a new [`Interval`] of a NegativeExpr  that has the existing `interval` given that
    /// given the input interval is known to be `children`.
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];

        if child_interval.lower() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.upper() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.lower() == &ScalarValue::Int64(Some(i64::MIN))
            || child_interval.upper() == &ScalarValue::Int64(Some(i64::MIN))
        {
            return Err(SparkError::ArithmeticOverflow {
                from_type: "long".to_string(),
            }
            .into());
        }

        let negated_interval = Interval::try_new(
            interval.upper().arithmetic_negate()?,
            interval.lower().arithmetic_negate()?,
        )?;

        Ok(child_interval
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    /// A [`NegativeExpr`] reverses its child's ordering and reflects its child's range
    /// about zero wherever negation is strictly decreasing over that range.
    ///
    /// Spark's legacy (non-ANSI) mode negates an integer with two's-complement wrapping,
    /// where the minimum of the type is its own negation. An ascending
    /// `[i32::MIN, i32::MIN + 1]` then becomes the still ascending `[i32::MIN, i32::MAX]`,
    /// and a `[i32::MIN, -1]` range yields both `i32::MIN` and positive values. A range
    /// that may hold the minimum therefore claims no ordering and the child type's full
    /// range.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let child = &children[0];
        let unbounded = || Interval::make_unbounded(&child.range.data_type());
        let strictly_decreasing = self.negation_is_strictly_decreasing(&child.range);

        let range = if strictly_decreasing {
            // `ScalarValue::arithmetic_negate` reports an error rather than a negation
            // for an unsigned bound, for the minimum of a signed integer, and for the
            // null bound of a decimal or interval range.
            child.range.arithmetic_negate().or_else(|_| unbounded())
        } else {
            unbounded()
        }?;

        Ok(ExprProperties {
            sort_properties: match child.sort_properties {
                SortProperties::Singleton => SortProperties::Singleton,
                order if strictly_decreasing => -order,
                _ => SortProperties::Unordered,
            },
            range,
            preserves_lex_ordering: false,
        })
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::*, buffer::NullBuffer, compute::SortOptions, datatypes::*};
    use datafusion::{
        logical_expr::sort_properties::SortProperties,
        physical_expr::{
            expressions::{Column, Literal},
            EquivalenceProperties, PhysicalSortExpr,
        },
        physical_plan::ColumnarValue,
    };

    fn eval_array(array: ArrayRef, fail_on_error: bool) -> Result<ColumnarValue> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        NegativeExpr::new(Arc::new(Column::new("a", 0)), fail_on_error).evaluate(&batch)
    }

    fn eval_scalar(scalar: ScalarValue, fail_on_error: bool) -> Result<ColumnarValue> {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        NegativeExpr::new(Arc::new(Literal::new(scalar)), fail_on_error).evaluate(&batch)
    }

    fn assert_spark_overflow(err: DataFusionError, expected_from_type: &str) {
        if let DataFusionError::External(ref e) = err {
            if let Some(SparkError::ArithmeticOverflow { from_type }) =
                e.downcast_ref::<SparkError>()
            {
                assert_eq!(from_type, expected_from_type);
                return;
            }
        }
        panic!(
            "Expected SparkError::ArithmeticOverflow {{ from_type: {:?} }}, got: {:?}",
            expected_from_type, err
        );
    }

    /// Negate `[min, other]` in ANSI mode with slot 0 marked null, and assert the
    /// null slot is skipped instead of raising a spurious overflow.
    fn assert_null_min_slot_is_skipped<T: ArrowPrimitiveType>(
        min: T::Native,
        other: T::Native,
        negated_other: T::Native,
    ) {
        let nulls = NullBuffer::from(vec![false, true]);
        let array: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
            vec![min, other].into(),
            Some(nulls),
        ));
        let ColumnarValue::Array(result) = eval_array(array, true).unwrap() else {
            panic!("expected array result")
        };
        let result = result.as_primitive::<T>();
        assert!(result.is_null(0));
        assert_eq!(result.value(1), negated_other);
    }

    /// A MIN sentinel left behind in a null slot (by filter, slice or FFI) must not raise a
    /// spurious ANSI overflow: the overflow check has to consult the null buffer and skip
    /// invalid slots. Each case below places `MIN` in a null slot.
    #[test]
    fn test_ansi_null_slot_with_min_values_does_not_overflow() {
        assert_null_min_slot_is_skipped::<Int8Type>(i8::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int16Type>(i16::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int32Type>(i32::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int64Type>(i64::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<IntervalYearMonthType>(i32::MIN, 7, -7);
        // `IntervalDayTime::MIN` is both components at `i32::MIN`.
        assert_null_min_slot_is_skipped::<IntervalDayTimeType>(
            IntervalDayTime::MIN,
            IntervalDayTime::new(1, 2),
            IntervalDayTime::new(-1, -2),
        );
    }

    #[test]
    fn test_ansi_valid_min_values_raise_exact_spark_overflow_errors() {
        let arr_i8: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN]));
        assert_spark_overflow(eval_array(arr_i8, true).unwrap_err(), "byte");

        let arr_i16: ArrayRef = Arc::new(Int16Array::from(vec![i16::MIN]));
        assert_spark_overflow(eval_array(arr_i16, true).unwrap_err(), "short");

        let arr_i32: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_i32, true).unwrap_err(), "integer");

        let arr_i64: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
        assert_spark_overflow(eval_array(arr_i64, true).unwrap_err(), "long");

        let arr_ym: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_ym, true).unwrap_err(), "interval");

        let arr_dt: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::MIN]));
        assert_spark_overflow(eval_array(arr_dt, true).unwrap_err(), "interval");
    }

    /// A single `DayTime` component at `i32::MIN` overflows: `neg` checks each component
    /// (`neg_wrapping` delegates to `neg` for every non-integer type,
    /// `downcast_integer! { ..., _ => neg(array) }`). These surface as Spark overflows like
    /// every other ANSI overflow in this expression.
    #[test]
    fn test_ansi_interval_day_time_component_overflow_maps_to_spark_error() {
        for value in [
            IntervalDayTime::new(i32::MIN, 0),
            IntervalDayTime::new(0, i32::MIN),
        ] {
            let array: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![value]));
            assert_spark_overflow(eval_array(array, true).unwrap_err(), "interval");
        }
    }

    /// Negate `[min, null]` in legacy mode and assert `min` wraps to itself.
    fn assert_legacy_wraps_min<T: ArrowPrimitiveType>(min: T::Native) {
        let array: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
            vec![min, T::Native::default()].into(),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let ColumnarValue::Array(result) = eval_array(array, false).unwrap() else {
            panic!("expected array result")
        };
        let result = result.as_primitive::<T>();
        assert_eq!(result.value(0), min);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_legacy_mode_wraps_min_values() {
        assert_legacy_wraps_min::<Int8Type>(i8::MIN);
        assert_legacy_wraps_min::<Int16Type>(i16::MIN);
        assert_legacy_wraps_min::<Int32Type>(i32::MIN);
        assert_legacy_wraps_min::<Int64Type>(i64::MIN);
    }

    #[test]
    fn test_mixed_ordinary_values() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(-7), Some(0), Some(12), None]));

        // ANSI mode
        let ColumnarValue::Array(res_ansi) = eval_array(Arc::clone(&arr), true).unwrap() else {
            panic!("expected array result")
        };
        assert_eq!(
            res_ansi.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(7), Some(0), Some(-12), None])
        );

        // Legacy mode
        let ColumnarValue::Array(res_legacy) = eval_array(Arc::clone(&arr), false).unwrap() else {
            panic!("expected array result")
        };
        assert_eq!(
            res_legacy.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(7), Some(0), Some(-12), None])
        );
    }

    #[test]
    fn test_interval_month_day_nano_preserves_existing_dispatch() {
        let arr: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 2, 3)),
            None,
        ]));
        let ColumnarValue::Array(res) = eval_array(arr, true).unwrap() else {
            panic!("expected array result")
        };
        let p = res.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(p.value(0), IntervalMonthDayNano::new(-1, -2, -3));
        assert!(p.is_null(1));
    }

    #[test]
    fn test_scalar_negation() {
        // Valid scalar
        let ColumnarValue::Scalar(res_valid) =
            eval_scalar(ScalarValue::Int32(Some(42)), true).unwrap()
        else {
            panic!("expected scalar result")
        };
        assert_eq!(res_valid, ScalarValue::Int32(Some(-42)));

        // Null scalar
        let ColumnarValue::Scalar(res_null) = eval_scalar(ScalarValue::Int32(None), true).unwrap()
        else {
            panic!("expected scalar result")
        };
        assert_eq!(res_null, ScalarValue::Int32(None));

        // MIN scalar overflows in ANSI, with the same messages as the array path
        for (scalar, from_type) in [
            (ScalarValue::Int8(Some(i8::MIN)), "byte"),
            (ScalarValue::Int16(Some(i16::MIN)), "short"),
            (ScalarValue::Int32(Some(i32::MIN)), "integer"),
            (ScalarValue::Int64(Some(i64::MIN)), "long"),
        ] {
            assert_spark_overflow(eval_scalar(scalar, true).unwrap_err(), from_type);
        }
    }

    fn ordered(descending: bool, nulls_first: bool) -> SortProperties {
        SortProperties::Ordered(SortOptions {
            descending,
            nulls_first,
        })
    }

    fn int32_interval(lower: i32, upper: i32) -> Interval {
        Interval::try_new(
            ScalarValue::Int32(Some(lower)),
            ScalarValue::Int32(Some(upper)),
        )
        .unwrap()
    }

    fn unbounded(data_type: &DataType) -> Interval {
        Interval::make_unbounded(data_type).unwrap()
    }

    fn child_properties(sort_properties: SortProperties, range: Interval) -> ExprProperties {
        ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering: true,
        }
    }

    fn negate_properties(child: ExprProperties, fail_on_error: bool) -> Result<ExprProperties> {
        NegativeExpr::new(Arc::new(Column::new("a", 0)), fail_on_error).get_properties(&[child])
    }

    /// Negation is strictly decreasing over `[1, 10]`, so it flips `descending`. Nulls stay
    /// nulls, so `nulls_first` carries over.
    #[test]
    fn test_get_properties_reverses_child_ordering() {
        for (child, expected) in [
            (ordered(false, true), ordered(true, true)),
            (ordered(true, false), ordered(false, false)),
        ] {
            let props =
                negate_properties(child_properties(child, int32_interval(1, 10)), false).unwrap();
            assert_eq!(props.sort_properties, expected);
        }
    }

    #[test]
    fn test_get_properties_negates_child_range() {
        let props = negate_properties(
            child_properties(SortProperties::Unordered, int32_interval(1, 10)),
            false,
        )
        .unwrap();
        assert_eq!(props.range, int32_interval(-10, -1));
        assert!(!props.preserves_lex_ordering);
    }

    /// Legacy mode negates `[i32::MIN, i32::MIN + 1]` to `[i32::MIN, i32::MAX]`, which is
    /// still ascending, so an ascending child supports no ordering claim and the reflected
    /// bounds do not hold either.
    #[test]
    fn test_legacy_get_properties_drops_ordering_when_range_holds_int_min() {
        let props = negate_properties(
            child_properties(ordered(false, true), int32_interval(i32::MIN, i32::MIN + 1)),
            false,
        )
        .unwrap();
        assert_eq!(props.sort_properties, SortProperties::Unordered);
        assert_eq!(props.range, unbounded(&DataType::Int32));
    }

    /// `[i32::MIN + 1, 10]` excludes the wrapping order boundary at `i32::MIN`, so legacy
    /// mode still reverses the ordering and reflects the range.
    #[test]
    fn test_legacy_get_properties_reverses_ordering_when_range_excludes_int_min() {
        let props = negate_properties(
            child_properties(ordered(false, true), int32_interval(i32::MIN + 1, 10)),
            false,
        )
        .unwrap();
        assert_eq!(props.sort_properties, ordered(true, true));
        assert_eq!(props.range, int32_interval(-10, i32::MAX));
    }

    /// Negating a constant yields a constant even where it wraps: `-i32::MIN` is `i32::MIN`,
    /// which is still a single value.
    #[test]
    fn test_legacy_get_properties_keeps_a_singleton_child_singleton() {
        let props = negate_properties(
            child_properties(
                SortProperties::Singleton,
                int32_interval(i32::MIN, i32::MIN),
            ),
            false,
        )
        .unwrap();
        assert_eq!(props.sort_properties, SortProperties::Singleton);
        assert_eq!(props.range, unbounded(&DataType::Int32));
    }

    /// `ExprProperties::new_unknown` reports a `Null` range whatever the expression's real
    /// type, and that is what every expression without its own `get_properties` yields, so a
    /// `Null` range cannot rule out wrapping.
    #[test]
    fn test_legacy_get_properties_claims_no_ordering_for_an_untyped_range() {
        let props = negate_properties(
            child_properties(ordered(false, true), ExprProperties::new_unknown().range),
            false,
        )
        .unwrap();
        assert_eq!(props.sort_properties, SortProperties::Unordered);
        assert_eq!(props.range, unbounded(&DataType::Null));
    }

    /// ANSI mode raises an overflow error instead of wrapping, so a range reaching down to
    /// `i32::MIN` still reverses the ordering. Its reflection is not representable, so the
    /// full range is reported.
    #[test]
    fn test_ansi_get_properties_reverses_ordering_when_range_holds_int_min() {
        let props = negate_properties(
            child_properties(ordered(false, true), int32_interval(i32::MIN, 10)),
            true,
        )
        .unwrap();
        assert_eq!(props.sort_properties, ordered(true, true));
        assert_eq!(props.range, unbounded(&DataType::Int32));
    }

    /// ANSI mode routes unsigned integers to `neg_wrapping` rather than to the
    /// overflow-checked `neg`, so `0` negates to itself there too and an unsigned range
    /// down to `0` supports no ordering claim in either mode.
    #[test]
    fn test_ansi_get_properties_drops_ordering_for_an_unsigned_range_holding_zero() {
        let props = negate_properties(
            child_properties(ordered(false, true), unbounded(&DataType::UInt32)),
            true,
        )
        .unwrap();
        assert_eq!(props.sort_properties, SortProperties::Unordered);
        assert_eq!(props.range, unbounded(&DataType::UInt32));
    }

    /// An unbounded decimal range is made of null decimal bounds, which
    /// `ScalarValue::arithmetic_negate` has no negation for. Reporting the full range keeps
    /// the ordering claim without turning a decimal negation into a planning error.
    #[test]
    fn test_get_properties_does_not_fail_on_an_unbounded_decimal_range() {
        let decimal = DataType::Decimal128(10, 2);
        let props = negate_properties(
            child_properties(ordered(false, true), unbounded(&decimal)),
            true,
        )
        .unwrap();
        assert_eq!(props.sort_properties, ordered(true, true));
        assert_eq!(props.range, unbounded(&decimal));
    }

    /// The premise the legacy guard in `get_properties` rests on, executed: negating an
    /// ascending column that reaches `i32::MIN` leaves it ascending rather than descending.
    #[test]
    fn test_legacy_evaluation_of_int_min_leaves_an_ascending_column_ascending() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN, i32::MIN + 1]));
        let ColumnarValue::Array(result) = eval_array(array, false).unwrap() else {
            panic!("expected array result")
        };
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(result, &Int32Array::from(vec![i32::MIN, i32::MAX]));
        assert!(result.value(0) < result.value(1));
    }

    /// Sorts `a` ascending and returns what `EquivalenceProperties`, the caller that
    /// actually reaches `get_properties`, derives for `-a`.
    fn negation_of_ascending_column(fail_on_error: bool) -> SortProperties {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let column = Arc::new(Column::new("a", 0));
        let mut eq_properties = EquivalenceProperties::new(schema);
        eq_properties.add_ordering([PhysicalSortExpr::new(
            Arc::clone(&column) as Arc<dyn PhysicalExpr>,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )]);

        let negated: Arc<dyn PhysicalExpr> = Arc::new(NegativeExpr::new(column, fail_on_error));
        eq_properties.get_expr_properties(negated).sort_properties
    }

    /// ANSI negation is strictly decreasing, so sorting `a` ascending must not let
    /// `EquivalenceProperties` conclude that `-a` is ascending too.
    #[test]
    fn test_ansi_equivalence_properties_report_negation_as_descending() {
        assert_eq!(negation_of_ascending_column(true), ordered(true, true));
    }

    /// The column's range is unbounded, so legacy negation of it may wrap and
    /// `EquivalenceProperties` must be given no ordering for `-a` at all.
    #[test]
    fn test_legacy_equivalence_properties_report_negation_as_unordered() {
        assert_eq!(
            negation_of_ascending_column(false),
            SortProperties::Unordered
        );
    }

    #[test]
    fn test_map_neg_error_preserves_non_overflow_errors() {
        let err = map_neg_error(
            ArrowError::InvalidArgumentError("test custom error".to_string()),
            "integer",
        );
        assert!(
            matches!(&err, DataFusionError::ArrowError(inner, _)
                if matches!(inner.as_ref(), ArrowError::InvalidArgumentError(msg) if msg == "test custom error")),
            "expected the ArrowError to pass through unchanged, got: {err:?}"
        );
    }
}
