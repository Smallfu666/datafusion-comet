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

use arrow::array::{Array, ArrowNativeTypeOp, BooleanBufferBuilder, PrimitiveArray};
use arrow::array::{ArrayRef, AsArray};

use crate::{arithmetic_overflow_error_with_supp, divide_by_zero_error, EvalMode, SparkError};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type,
};
use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

pub fn try_arithmetic_kernel<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: &str,
    is_ansi_mode: bool,
    type_name: &str,
    is_binary_overflow: bool,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
    T::Native: std::fmt::Display,
{
    let (symbol, fn_name, is_div) = match op {
        "checked_add" => ("+", "try_add", false),
        "checked_sub" => ("-", "try_subtract", false),
        "checked_mul" => ("*", "try_multiply", false),
        "checked_div" => ("/", "try_divide", true),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unsupported operation: {:?}",
                op
            )))
        }
    };

    let func = |l: T::Native, r: T::Native| match op {
        "checked_add" => l.add_checked(r),
        "checked_sub" => l.sub_checked(r),
        "checked_mul" => l.mul_checked(r),
        "checked_div" => l.div_checked(r),
        _ => unreachable!(),
    };

    checked_binary(
        left,
        right,
        type_name,
        symbol,
        fn_name,
        is_binary_overflow,
        is_ansi_mode,
        is_div,
        func,
    )
}

#[allow(clippy::too_many_arguments)]
fn checked_binary<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    type_name: &str,
    symbol: &str,
    fn_name: &str,
    is_binary_overflow: bool,
    is_ansi_mode: bool,
    is_div: bool,
    op: F,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
    T::Native: std::fmt::Display,
    F: Fn(T::Native, T::Native) -> Result<T::Native, ArrowError>,
{
    let len = left.len();
    let lhs = &left.values()[..len];
    let rhs = &right.values()[..len];
    let nulls = NullBuffer::union(left.nulls(), right.nulls());

    let mut values = vec![T::Native::default(); len];
    let mut overflowed: Vec<usize> = Vec::new();

    for (i, (out, (&l, &r))) in values.iter_mut().zip(lhs.iter().zip(rhs)).enumerate() {
        if let Some(ref n) = nulls {
            if !n.is_valid(i) {
                continue;
            }
        }
        match op(l, r) {
            Ok(v) => *out = v,
            Err(_) => {
                if !is_ansi_mode {
                    overflowed.push(i);
                } else {
                    return if is_div && r.is_zero() {
                        Err(divide_by_zero_error().into())
                    } else if is_binary_overflow {
                        Err(SparkError::BinaryArithmeticOverflow {
                            value1: l.to_string(),
                            symbol: symbol.to_string(),
                            value2: r.to_string(),
                            function_name: fn_name.to_string(),
                        }
                        .into())
                    } else {
                        Err(arithmetic_overflow_error_with_supp(type_name, fn_name).into())
                    };
                }
            }
        }
    }

    let nulls = if overflowed.is_empty() {
        nulls
    } else {
        let mut validity = BooleanBufferBuilder::new(len);
        match &nulls {
            Some(n) => validity.append_buffer(n.inner()),
            None => validity.append_n(len, true),
        }
        for i in overflowed {
            validity.set_bit(i, false);
        }
        Some(NullBuffer::new(validity.finish()))
    };

    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)) as ArrayRef)
}
pub fn checked_add(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_add", eval_mode)
}

pub fn checked_sub(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_sub", eval_mode)
}

pub fn checked_mul(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_mul", eval_mode)
}

pub fn checked_div(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_div", eval_mode)
}

fn checked_arithmetic_internal(
    args: &[ColumnarValue],
    data_type: &DataType,
    op: &str,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];

    let is_ansi_mode = match eval_mode {
        EvalMode::Try => false,
        EvalMode::Ansi => true,
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unsupported mode : {:?}",
                eval_mode
            )))
        }
    };

    let (left_arr, right_arr): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };

    // Rust only supports checked_arithmetic on numeric types
    let result_array = match data_type {
        DataType::Int8 => try_arithmetic_kernel::<Int8Type>(
            left_arr.as_primitive::<Int8Type>(),
            right_arr.as_primitive::<Int8Type>(),
            op,
            is_ansi_mode,
            "byte",
            true,
        ),
        DataType::Int16 => try_arithmetic_kernel::<Int16Type>(
            left_arr.as_primitive::<Int16Type>(),
            right_arr.as_primitive::<Int16Type>(),
            op,
            is_ansi_mode,
            "short",
            true,
        ),
        DataType::Int32 => try_arithmetic_kernel::<Int32Type>(
            left_arr.as_primitive::<Int32Type>(),
            right_arr.as_primitive::<Int32Type>(),
            op,
            is_ansi_mode,
            "integer",
            false,
        ),
        DataType::Int64 => try_arithmetic_kernel::<Int64Type>(
            left_arr.as_primitive::<Int64Type>(),
            right_arr.as_primitive::<Int64Type>(),
            op,
            is_ansi_mode,
            "long",
            false,
        ),
        // Spark always casts division operands to floats
        DataType::Float16 if (op == "checked_div") => try_arithmetic_kernel::<Float16Type>(
            left_arr.as_primitive::<Float16Type>(),
            right_arr.as_primitive::<Float16Type>(),
            op,
            is_ansi_mode,
            "float",
            false,
        ),
        DataType::Float32 if (op == "checked_div") => try_arithmetic_kernel::<Float32Type>(
            left_arr.as_primitive::<Float32Type>(),
            right_arr.as_primitive::<Float32Type>(),
            op,
            is_ansi_mode,
            "float",
            false,
        ),
        DataType::Float64 if (op == "checked_div") => try_arithmetic_kernel::<Float64Type>(
            left_arr.as_primitive::<Float64Type>(),
            right_arr.as_primitive::<Float64Type>(),
            op,
            is_ansi_mode,
            "double",
            false,
        ),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type: {:?}",
            data_type
        ))),
    };

    Ok(ColumnarValue::Array(result_array?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};

    fn int32_args(left: Vec<Option<i32>>, right: Vec<Option<i32>>) -> Vec<ColumnarValue> {
        vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(left))),
            ColumnarValue::Array(Arc::new(Int32Array::from(right))),
        ]
    }

    fn as_int32(value: ColumnarValue) -> Int32Array {
        let ColumnarValue::Array(array) = value else {
            unreachable!()
        };
        array.as_primitive::<Int32Type>().clone()
    }

    #[test]
    fn test_checked_add_propagates_nulls() {
        let args = int32_args(
            vec![Some(1), None, Some(3), None],
            vec![Some(10), Some(20), None, None],
        );
        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![Some(11), None, None, None]));
    }

    #[test]
    fn test_checked_add_overflow_is_null_in_try_mode() {
        let args = int32_args(vec![Some(i32::MAX), Some(1)], vec![Some(1), Some(1)]);
        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));
    }

    #[test]
    fn test_checked_add_overflow_errors_in_ansi_mode() {
        let args = int32_args(vec![Some(i32::MAX)], vec![Some(1)]);
        assert!(checked_add(&args, &DataType::Int32, EvalMode::Ansi).is_err());
    }

    #[test]
    fn test_checked_sub_and_mul_overflow() {
        let args = int32_args(vec![Some(i32::MIN), Some(5)], vec![Some(1), Some(3)]);
        let result = as_int32(checked_sub(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));

        let args = int32_args(vec![Some(i32::MAX), Some(5)], vec![Some(2), Some(3)]);
        let result = as_int32(checked_mul(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(15)]));
    }

    #[test]
    fn test_checked_div_by_zero() {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(1.0), Some(8.0)]))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(0.0), Some(2.0)]))),
        ];
        let ColumnarValue::Array(array) =
            checked_div(&args, &DataType::Float64, EvalMode::Try).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(
            array.as_primitive::<Float64Type>(),
            &Float64Array::from(vec![None, Some(4.0)])
        );

        assert!(checked_div(&args, &DataType::Float64, EvalMode::Ansi).is_err());
    }

    /// A null slot may hold any value in its underlying buffer (e.g. after a filter or a slice).
    /// Such a row must not overflow-error in ANSI mode, and its result slot must read back as a
    /// null holding the default value.
    #[test]
    fn test_null_row_with_garbage_value_does_not_error_in_ansi_mode() {
        let nulls = NullBuffer::from(vec![false, true]);
        let left = Int32Array::new(vec![i32::MAX, 1].into(), Some(nulls));
        let right = Int32Array::from(vec![Some(1), Some(1)]);
        let args = vec![
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ];

        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));
        assert_eq!(result.values()[0], 0);
    }

    #[test]
    fn test_ansi_overflow_errors() {
        use arrow::array::{Int16Array, Int64Array, Int8Array};

        // Int8 (Byte) -> BinaryArithmeticOverflow
        let left8 = ColumnarValue::Array(Arc::new(Int8Array::from(vec![127])));
        let right8 = ColumnarValue::Array(Arc::new(Int8Array::from(vec![1])));
        let err8 = checked_add(&[left8, right8], &DataType::Int8, EvalMode::Ansi).unwrap_err();
        assert!(err8.to_string().contains("[BINARY_ARITHMETIC_OVERFLOW] 127 + 1 caused overflow. Use `try_add` to tolerate overflow"));

        // Int16 (Short) -> BinaryArithmeticOverflow
        let left16 = ColumnarValue::Array(Arc::new(Int16Array::from(vec![32767])));
        let right16 = ColumnarValue::Array(Arc::new(Int16Array::from(vec![1])));
        let err16 = checked_add(&[left16, right16], &DataType::Int16, EvalMode::Ansi).unwrap_err();
        assert!(err16.to_string().contains("[BINARY_ARITHMETIC_OVERFLOW] 32767 + 1 caused overflow. Use `try_add` to tolerate overflow"));

        // Int32 -> ArithmeticOverflow ("integer")
        let left32 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![i32::MAX])));
        let right32 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])));
        let err32 = checked_add(&[left32, right32], &DataType::Int32, EvalMode::Ansi).unwrap_err();
        assert!(err32
            .to_string()
            .contains("[ARITHMETIC_OVERFLOW] integer overflow"));

        // Int64 -> ArithmeticOverflow ("long")
        let left64 = ColumnarValue::Array(Arc::new(Int64Array::from(vec![i64::MAX])));
        let right64 = ColumnarValue::Array(Arc::new(Int64Array::from(vec![1])));
        let err64 = checked_add(&[left64, right64], &DataType::Int64, EvalMode::Ansi).unwrap_err();
        assert!(err64
            .to_string()
            .contains("[ARITHMETIC_OVERFLOW] long overflow"));
    }

    #[test]
    fn test_checked_div_null_with_zero_buffer_in_ansi_mode() {
        let left = Int32Array::from(vec![Some(10), None]);
        let right_nulls = NullBuffer::from(vec![true, false]);
        // Slot 1 is null, but its underlying buffer holds 0
        let right = Int32Array::new(vec![2, 0].into(), Some(right_nulls));
        let args = vec![
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ];

        let result = as_int32(checked_div(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![Some(5), None]));
    }
}
