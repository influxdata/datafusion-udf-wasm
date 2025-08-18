use datafusion::{arrow::datatypes::DataType, error::DataFusionError, logical_expr as df_expr};

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

impl From<wit_types::DataType> for DataType {
    fn from(value: wit_types::DataType) -> Self {
        use wit_types::DataType;

        match value {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Uint8 => Self::UInt8,
            DataType::Uint16 => Self::UInt16,
            DataType::Uint32 => Self::UInt32,
            DataType::Uint64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
        }
    }
}

impl TryFrom<DataType> for wit_types::DataType {
    type Error = DataFusionError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        Ok(match value {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::Uint8,
            DataType::UInt16 => Self::Uint16,
            DataType::UInt32 => Self::Uint32,
            DataType::UInt64 => Self::Uint64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "serialize {value:?}"
                )));
            }
        })
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

impl From<wit_types::TypeSignature> for df_expr::TypeSignature {
    fn from(value: wit_types::TypeSignature) -> Self {
        use wit_types::TypeSignature;

        match value {
            TypeSignature::Variadic(data_types) => {
                Self::Variadic(data_types.into_iter().map(From::from).collect())
            }
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform((n, data_types)) => {
                Self::Uniform(n as usize, data_types.into_iter().map(From::from).collect())
            }
            TypeSignature::Exact(data_types) => {
                Self::Exact(data_types.into_iter().map(From::from).collect())
            }
            TypeSignature::Comparable(n) => Self::Comparable(n as usize),
            TypeSignature::Any(n) => Self::Any(n as usize),
            TypeSignature::ArraySignature(array_function_signature) => {
                Self::ArraySignature(array_function_signature.into())
            }
            TypeSignature::Numeric(n) => Self::Numeric(n as usize),
            TypeSignature::String(n) => Self::String(n as usize),
            TypeSignature::Nullary => Self::Nullary,
        }
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

impl From<wit_types::Signature> for df_expr::Signature {
    fn from(value: wit_types::Signature) -> Self {
        Self {
            type_signature: value.type_signature.into(),
            volatility: value.volatility.into(),
        }
    }
}
