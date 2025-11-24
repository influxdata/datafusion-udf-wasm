//! Conversion routes from/to WIT types.
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode},
};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_udf_wasm_arrow2bytes::{array2bytes, bytes2array, bytes2datatype, datatype2bytes};

use crate::{
    bindings::exports::datafusion_udf_wasm::udf::types::{self as wit_types},
    conversion::limits::{CheckedFrom, CheckedInto},
    error::DataFusionResultExt,
};

pub mod limits;

impl CheckedFrom<wit_types::DataFusionError> for DataFusionError {
    fn checked_from(
        value: wit_types::DataFusionError,
        mut token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        use wit_types::DataFusionErrorKind;

        let mut e = match value.kind {
            DataFusionErrorKind::NotImplemented(msg) => {
                token.check_aux_string(&msg)?;
                Self::NotImplemented(msg)
            }
            DataFusionErrorKind::Internal(msg) => {
                token.check_aux_string(&msg)?;
                Self::Internal(msg)
            }
            DataFusionErrorKind::Plan(msg) => {
                token.check_aux_string(&msg)?;
                Self::Plan(msg)
            }
            DataFusionErrorKind::Configuration(msg) => {
                token.check_aux_string(&msg)?;
                Self::Configuration(msg)
            }
            DataFusionErrorKind::Execution(msg) => {
                token.check_aux_string(&msg)?;
                Self::Execution(msg)
            }
        };

        // context chain is stored "top-level to inner-level", but we assemble the types inner-to-outer
        for context in value.context.into_iter().rev() {
            token = token.sub()?;
            token.check_aux_string(&context)?;
            e = e.context(context);
        }

        Ok(e)
    }
}

/// Check [`IntervalUnit`] for complexity.
fn check_interval_unit(
    iu: &IntervalUnit,
    token: &limits::ComplexityToken,
) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    match iu {
        IntervalUnit::YearMonth | IntervalUnit::DayTime | IntervalUnit::MonthDayNano => {
            token.no_recursion();
            Ok(())
        }
    }
}

/// Check [`TimeUnit`] for complexity.
fn check_time_unit(
    tu: &TimeUnit,
    token: &limits::ComplexityToken,
) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    match tu {
        TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond => {
            token.no_recursion();
            Ok(())
        }
    }
}

/// Check [`UnionFields`] complexity.
fn check_union_fields(
    ufields: &UnionFields,
    token: &limits::ComplexityToken,
) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    for (_idx, field) in ufields.iter() {
        check_field(field, &token)?;
    }
    Ok(())
}

/// Check [`UnionMode`] complexity.
fn check_union_mode(
    umode: &UnionMode,
    token: &limits::ComplexityToken,
) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    match umode {
        UnionMode::Sparse | UnionMode::Dense => {
            token.no_recursion();
            Ok(())
        }
    }
}

/// Check [`DataType`] complexity.
fn check_data_type(
    dt: &DataType,
    token: &limits::ComplexityToken,
) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    match dt {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            token.no_recursion();
            Ok(())
        }
        DataType::Timestamp(tu, tz) => {
            check_time_unit(tu, &token)?;
            if let Some(tz) = tz {
                token.check_identifier(tz.as_ref())?;
            }
            Ok(())
        }
        DataType::Time32(tu) | DataType::Time64(tu) | DataType::Duration(tu) => {
            check_time_unit(tu, &token)
        }
        DataType::Interval(iu) => check_interval_unit(iu, &token),
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => check_field(field, &token),
        DataType::Struct(fields) => {
            for field in fields {
                check_field(field, &token)?;
            }
            Ok(())
        }
        DataType::Union(ufields, umode) => {
            check_union_fields(ufields, &token)?;
            check_union_mode(umode, &token)?;
            Ok(())
        }
        DataType::Dictionary(dt1, dt2) => {
            check_data_type(dt1, &token)?;
            check_data_type(dt2, &token)?;
            Ok(())
        }
        DataType::RunEndEncoded(field1, field2) => {
            check_field(field1, &token)?;
            check_field(field2, &token)?;
            Ok(())
        }
    }
}

/// Check [`Field`] complexity.
fn check_field(field: &Field, token: &limits::ComplexityToken) -> datafusion_common::Result<()> {
    let token = token.sub()?;

    token.check_identifier(field.name())?;
    check_data_type(field.data_type(), &token)?;

    for (k, v) in field.metadata() {
        token.check_identifier(k)?;
        token.check_aux_string(v)?;
    }

    Ok(())
}

impl CheckedFrom<wit_types::DataType> for DataType {
    fn checked_from(
        value: wit_types::DataType,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        let dt = bytes2datatype(&value.arrow_ipc_schema)?;
        check_data_type(&dt, &token)?;
        Ok(dt)
    }
}

impl From<DataType> for wit_types::DataType {
    fn from(value: DataType) -> Self {
        Self {
            arrow_ipc_schema: datatype2bytes(value),
        }
    }
}

impl From<Field> for wit_types::Field {
    fn from(value: Field) -> Self {
        Self {
            name: value.name().clone(),
            data_type: value.data_type().clone().into(),
            nullable: value.is_nullable(),
            dict_is_ordered: value.dict_is_ordered().unwrap_or_default(),
            metadata: value
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl CheckedFrom<wit_types::ArrayFunctionSignature> for datafusion_expr::ArrayFunctionSignature {
    fn checked_from(
        value: wit_types::ArrayFunctionSignature,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        use wit_types::ArrayFunctionSignature;

        token.no_recursion();

        Ok(match value {
            ArrayFunctionSignature::RecursiveArray => Self::RecursiveArray,
            ArrayFunctionSignature::MapArray => Self::MapArray,
        })
    }
}

impl CheckedFrom<wit_types::TypeSignature> for datafusion_expr::TypeSignature {
    fn checked_from(
        value: wit_types::TypeSignature,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        use wit_types::TypeSignature;

        Ok(match value {
            TypeSignature::Variadic(data_types) => Self::Variadic(
                data_types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, dt)| {
                        dt.checked_into(&token)
                            .with_context(|| format!("child {idx}"))
                    })
                    .collect::<Result<_, _>>()
                    .context("variadic signature")?,
            ),
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform((n, data_types)) => Self::Uniform(
                n as usize,
                data_types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, dt)| {
                        dt.checked_into(&token)
                            .with_context(|| format!("child {idx}"))
                    })
                    .collect::<Result<_, _>>()
                    .context("uniform signature")?,
            ),
            TypeSignature::Exact(data_types) => Self::Exact(
                data_types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, dt)| {
                        dt.checked_into(&token)
                            .with_context(|| format!("child {idx}"))
                    })
                    .collect::<Result<_, _>>()
                    .context("exact signature")?,
            ),
            TypeSignature::Comparable(n) => Self::Comparable(n as usize),
            TypeSignature::Any(n) => Self::Any(n as usize),
            TypeSignature::ArraySignature(array_function_signature) => Self::ArraySignature(
                array_function_signature
                    .checked_into(&token)
                    .context("array signature")?,
            ),
            TypeSignature::Numeric(n) => Self::Numeric(n as usize),
            TypeSignature::String(n) => Self::String(n as usize),
            TypeSignature::Nullary => Self::Nullary,
        })
    }
}

impl CheckedFrom<wit_types::Volatility> for datafusion_expr::Volatility {
    fn checked_from(
        value: wit_types::Volatility,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        use wit_types::Volatility;

        token.no_recursion();

        Ok(match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        })
    }
}

impl CheckedFrom<wit_types::Signature> for datafusion_expr::Signature {
    fn checked_from(
        value: wit_types::Signature,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        Ok(Self {
            type_signature: value
                .type_signature
                .checked_into(&token)
                .context("type signature")?,
            volatility: value
                .volatility
                .checked_into(&token)
                .context("volatility")?,
        })
    }
}

impl From<ArrayRef> for wit_types::Array {
    fn from(value: ArrayRef) -> Self {
        Self {
            arrow_ipc_batch: array2bytes(value),
        }
    }
}

impl CheckedFrom<wit_types::Array> for ArrayRef {
    fn checked_from(
        value: wit_types::Array,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        let array = bytes2array(&value.arrow_ipc_batch)?;
        // we assume that the array data and the attached data type are in-sync, so we only gonna check the data type
        check_data_type(array.data_type(), &token)?;
        Ok(array)
    }
}

impl TryFrom<ScalarValue> for wit_types::ScalarValue {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        let array = value.to_array_of_size(1)?;

        Ok(Self {
            array: array.into(),
        })
    }
}

impl CheckedFrom<wit_types::ScalarValue> for ScalarValue {
    fn checked_from(
        value: wit_types::ScalarValue,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        let array: ArrayRef = value.array.checked_into(&token).context("array")?;
        if array.len() != 1 {
            return Err(DataFusionError::Internal(
                "scalar value must be array of len 1".to_owned(),
            ));
        }
        Self::try_from_array(&array, 0)
    }
}

impl TryFrom<ColumnarValue> for wit_types::ColumnarValue {
    type Error = DataFusionError;

    fn try_from(value: ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnarValue::Array(array) => Self::Array(array.into()),
            ColumnarValue::Scalar(scalar) => Self::Scalar(scalar.try_into()?),
        })
    }
}

impl CheckedFrom<wit_types::ColumnarValue> for ColumnarValue {
    fn checked_from(
        value: wit_types::ColumnarValue,
        token: limits::ComplexityToken,
    ) -> datafusion_common::Result<Self> {
        use wit_types::ColumnarValue;

        Ok(match value {
            ColumnarValue::Array(array) => {
                Self::Array(array.checked_into(&token).context("array")?)
            }
            ColumnarValue::Scalar(scalar) => {
                Self::Scalar(scalar.checked_into(&token).context("scalar")?)
            }
        })
    }
}

impl TryFrom<ScalarFunctionArgs> for wit_types::ScalarFunctionArgs {
    type Error = DataFusionError;

    fn try_from(value: ScalarFunctionArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            args: value
                .args
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            arg_fields: value
                .arg_fields
                .into_iter()
                .map(|f| f.as_ref().clone().into())
                .collect(),
            number_rows: value.number_rows as u64,
            return_field: value.return_field.as_ref().clone().into(),
            config_options: value
                .config_options
                .entries()
                .into_iter()
                .filter_map(|e| {
                    let k = e.key;
                    let v = e.value?;
                    Some((k, v))
                })
                .collect(),
        })
    }
}
