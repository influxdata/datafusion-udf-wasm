use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{error::DataFusionError, scalar::ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_udf_wasm_arrow2bytes::{array2bytes, bytes2array, bytes2datatype, datatype2bytes};

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;

impl From<DataFusionError> for wit_types::DataFusionError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::NotImplemented(msg) => Self::NotImplemented(msg),
            DataFusionError::Internal(msg) => Self::Internal(msg),
            DataFusionError::Plan(msg) => Self::Plan(msg),
            DataFusionError::Configuration(msg) => Self::Configuration(msg),
            DataFusionError::Execution(msg) => Self::Execution(msg),
            _ => Self::NotImplemented(format!("serialize error: {value}")),
        }
    }
}

impl TryFrom<wit_types::DataType> for DataType {
    type Error = DataFusionError;

    fn try_from(value: wit_types::DataType) -> Result<Self, Self::Error> {
        let dt = bytes2datatype(&value.arrow_ipc_schema)?;
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

impl TryFrom<wit_types::Field> for FieldRef {
    type Error = DataFusionError;

    fn try_from(value: wit_types::Field) -> Result<Self, Self::Error> {
        let wit_types::Field {
            name,
            data_type,
            nullable,
            dict_is_ordered,
            metadata,
        } = value;

        Ok(Arc::new(
            Field::new(name, data_type.try_into()?, nullable)
                .with_dict_is_ordered(dict_is_ordered)
                .with_metadata(metadata.into_iter().collect()),
        ))
    }
}

impl TryFrom<datafusion_expr::ArrayFunctionSignature> for wit_types::ArrayFunctionSignature {
    type Error = DataFusionError;

    fn try_from(value: datafusion_expr::ArrayFunctionSignature) -> Result<Self, Self::Error> {
        use datafusion_expr::ArrayFunctionSignature;

        Ok(match value {
            ArrayFunctionSignature::Array { .. } => {
                return Err(DataFusionError::NotImplemented(
                    "ArrayFunctionSignature::Array".to_string(),
                ));
            }
            ArrayFunctionSignature::RecursiveArray => Self::RecursiveArray,
            ArrayFunctionSignature::MapArray => Self::MapArray,
        })
    }
}

impl TryFrom<datafusion_expr::TypeSignature> for wit_types::TypeSignature {
    type Error = DataFusionError;

    fn try_from(value: datafusion_expr::TypeSignature) -> Result<Self, Self::Error> {
        use datafusion_expr::TypeSignature;

        Ok(match value {
            TypeSignature::Variadic(data_types) => {
                Self::Variadic(data_types.into_iter().map(From::from).collect())
            }
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform(n, data_types) => {
                Self::Uniform((n as u64, data_types.into_iter().map(From::from).collect()))
            }
            TypeSignature::Exact(data_types) => {
                Self::Exact(data_types.into_iter().map(From::from).collect())
            }
            TypeSignature::Coercible(_coercions) => {
                return Err(DataFusionError::NotImplemented(
                    "serialize TypeSignature::Coercible".to_owned(),
                ));
            }
            TypeSignature::Comparable(n) => Self::Comparable(n as u64),
            TypeSignature::Any(n) => Self::Any(n as u64),
            TypeSignature::OneOf(_type_signatures) => {
                return Err(DataFusionError::NotImplemented(
                    "serialize TypeSignature::OneOf".to_owned(),
                ));
            }
            TypeSignature::ArraySignature(array_function_signature) => {
                Self::ArraySignature(array_function_signature.try_into()?)
            }
            TypeSignature::Numeric(n) => Self::Numeric(n as u64),
            TypeSignature::String(n) => Self::String(n as u64),
            TypeSignature::Nullary => Self::Nullary,
        })
    }
}

impl From<datafusion_expr::Volatility> for wit_types::Volatility {
    fn from(value: datafusion_expr::Volatility) -> Self {
        use datafusion_expr::Volatility;

        match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        }
    }
}

impl TryFrom<datafusion_expr::Signature> for wit_types::Signature {
    type Error = DataFusionError;

    fn try_from(value: datafusion_expr::Signature) -> Result<Self, Self::Error> {
        Ok(Self {
            type_signature: value.type_signature.try_into()?,
            volatility: value.volatility.into(),
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

impl TryFrom<wit_types::Array> for ArrayRef {
    type Error = DataFusionError;

    fn try_from(value: wit_types::Array) -> Result<Self, Self::Error> {
        let array = bytes2array(&value.arrow_ipc_batch)?;
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

impl TryFrom<wit_types::ScalarValue> for ScalarValue {
    type Error = DataFusionError;

    fn try_from(value: wit_types::ScalarValue) -> Result<Self, Self::Error> {
        let array = ArrayRef::try_from(value.array)?;
        if array.len() != 1 {
            return Err(DataFusionError::Internal(
                "scalar value must be array of len 1".to_owned(),
            ));
        }
        ScalarValue::try_from_array(&array, 0)
    }
}

impl TryFrom<wit_types::ColumnarValue> for ColumnarValue {
    type Error = DataFusionError;

    fn try_from(value: wit_types::ColumnarValue) -> Result<Self, Self::Error> {
        use wit_types::ColumnarValue;

        Ok(match value {
            ColumnarValue::Array(array) => Self::Array(array.try_into()?),
            ColumnarValue::Scalar(scalar) => Self::Scalar(scalar.try_into()?),
        })
    }
}

impl TryFrom<ColumnarValue> for wit_types::ColumnarValue {
    type Error = DataFusionError;

    fn try_from(value: ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnarValue::Array(array) => wit_types::ColumnarValue::Array(array.into()),
            ColumnarValue::Scalar(scalar) => wit_types::ColumnarValue::Scalar(scalar.try_into()?),
        })
    }
}

impl TryFrom<wit_types::ScalarFunctionArgs> for ScalarFunctionArgs {
    type Error = DataFusionError;

    fn try_from(value: wit_types::ScalarFunctionArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            args: value
                .args
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            arg_fields: value
                .arg_fields
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            number_rows: value.number_rows as usize,
            return_field: value.return_field.try_into()?,
        })
    }
}
