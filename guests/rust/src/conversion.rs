use datafusion::{arrow::datatypes::DataType, error::DataFusionError, logical_expr as df_expr};
use datafusion_udf_wasm_arrow2bytes::{bytes2datatype, datatype2bytes};

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
        bytes2datatype(&value.arrow_ipc_schema)
    }
}

impl From<DataType> for wit_types::DataType {
    fn from(value: DataType) -> Self {
        Self {
            arrow_ipc_schema: datatype2bytes(value),
        }
    }
}

impl TryFrom<df_expr::ArrayFunctionSignature> for wit_types::ArrayFunctionSignature {
    type Error = DataFusionError;

    fn try_from(value: df_expr::ArrayFunctionSignature) -> Result<Self, Self::Error> {
        use df_expr::ArrayFunctionSignature;

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

impl TryFrom<df_expr::TypeSignature> for wit_types::TypeSignature {
    type Error = DataFusionError;

    fn try_from(value: df_expr::TypeSignature) -> Result<Self, Self::Error> {
        use df_expr::TypeSignature;

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

impl From<df_expr::Volatility> for wit_types::Volatility {
    fn from(value: df_expr::Volatility) -> Self {
        use df_expr::Volatility;

        match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        }
    }
}

impl TryFrom<df_expr::Signature> for wit_types::Signature {
    type Error = DataFusionError;

    fn try_from(value: df_expr::Signature) -> Result<Self, Self::Error> {
        Ok(Self {
            type_signature: value.type_signature.try_into()?,
            volatility: value.volatility.into(),
        })
    }
}
