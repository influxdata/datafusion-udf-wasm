use datafusion::{arrow::datatypes::DataType, error::DataFusionError, logical_expr as df_expr};

use crate::bindings::exports::datafusion_udf_wasm::udf::types as wit_types;

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
            TypeSignature::Variadic(data_types) => Self::Variadic(
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform(n, data_types) => Self::Uniform((
                n as u64,
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            TypeSignature::Exact(data_types) => Self::Exact(
                data_types
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
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
