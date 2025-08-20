use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field},
    },
    error::DataFusionError,
    logical_expr::{self as df_expr, ColumnarValue, ScalarFunctionArgs},
    scalar::ScalarValue,
};
use datafusion_udf_wasm_arrow2bytes::{array2bytes, bytes2array, bytes2datatype, datatype2bytes};

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;

impl From<wit_types::DataFusionError> for DataFusionError {
    fn from(value: wit_types::DataFusionError) -> Self {
        use wit_types::DataFusionError;

        match value {
            DataFusionError::NotImplemented(msg) => Self::NotImplemented(msg),
            DataFusionError::Internal(msg) => Self::Internal(msg),
            DataFusionError::Plan(msg) => Self::Plan(msg),
            DataFusionError::Configuration(msg) => Self::Configuration(msg),
            DataFusionError::Execution(msg) => Self::Execution(msg),
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

impl From<wit_types::ArrayFunctionSignature> for df_expr::ArrayFunctionSignature {
    fn from(value: wit_types::ArrayFunctionSignature) -> Self {
        use wit_types::ArrayFunctionSignature;

        match value {
            ArrayFunctionSignature::RecursiveArray => Self::RecursiveArray,
            ArrayFunctionSignature::MapArray => Self::MapArray,
        }
    }
}

impl TryFrom<wit_types::TypeSignature> for df_expr::TypeSignature {
    type Error = DataFusionError;

    fn try_from(value: wit_types::TypeSignature) -> Result<Self, DataFusionError> {
        use wit_types::TypeSignature;

        Ok(match value {
            TypeSignature::Variadic(data_types) => Self::Variadic(
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform((n, data_types)) => Self::Uniform(
                n as usize,
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            TypeSignature::Exact(data_types) => Self::Exact(
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            TypeSignature::Comparable(n) => Self::Comparable(n as usize),
            TypeSignature::Any(n) => Self::Any(n as usize),
            TypeSignature::ArraySignature(array_function_signature) => {
                Self::ArraySignature(array_function_signature.into())
            }
            TypeSignature::Numeric(n) => Self::Numeric(n as usize),
            TypeSignature::String(n) => Self::String(n as usize),
            TypeSignature::Nullary => Self::Nullary,
        })
    }
}

impl From<wit_types::Volatility> for df_expr::Volatility {
    fn from(value: wit_types::Volatility) -> Self {
        use wit_types::Volatility;

        match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        }
    }
}

impl TryFrom<wit_types::Signature> for df_expr::Signature {
    type Error = DataFusionError;

    fn try_from(value: wit_types::Signature) -> Result<Self, Self::Error> {
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
        })
    }
}
