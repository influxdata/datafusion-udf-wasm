//! Evil payloads that try to break the sandbox when the UDF is called.
use std::{hash::Hash, sync::Arc};

use arrow::datatypes::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// UDF that executes a side effect.
struct SideEffect {
    /// Name.
    name: &'static str,

    /// Side effect.
    effect: Box<dyn Fn() + Send + Sync>,

    /// Signature of the UDF.
    ///
    /// We store this here because [`ScalarUDFImpl::signature`] requires us to return a reference.
    signature: Signature,
}

impl SideEffect {
    /// Create new side-effect UDF.
    fn new<F>(name: &'static str, effect: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            name,
            effect: Box::new(effect),
            signature: Signature::uniform(0, vec![], Volatility::Immutable),
        }
    }
}

impl std::fmt::Debug for SideEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            effect: _,
            signature,
        } = self;

        f.debug_struct("SideEffect")
            .field("name", name)
            .field("effect", &"<EFFECT>")
            .field("signature", signature)
            .finish()
    }
}

impl PartialEq<Self> for SideEffect {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SideEffect {}

impl Hash for SideEffect {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SideEffect {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        (self.effect)();

        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}

/// [Ackermann Function](https://en.wikipedia.org/wiki/Ackermann_function).
///
/// The Ackermann function is used here because it's not
/// [primitive recursive](https://en.wikipedia.org/wiki/Primitive_recursive_function) and thus it cannot be optimized
/// away into a for loop. Because the function is recursive and it won't be optimized away, a stack overflow can occur
/// if `n` or `m` is large enough.
fn ackermann(m: u128, n: u128) -> u128 {
    if m == 0 {
        n + 1
    } else if n == 0 {
        ackermann(m - 1, 1)
    } else {
        ackermann(m - 1, ackermann(m, n - 1))
    }
}

/// Returns our evil UDFs.
///
/// The passed `source` is ignored.
#[expect(clippy::unnecessary_wraps, reason = "public API through export! macro")]
pub(crate) fn udfs(_source: String) -> DataFusionResult<Vec<Arc<dyn ScalarUDFImpl>>> {
    Ok(vec![
        Arc::new(SideEffect::new("fillstderr", || {
            let s: String = std::iter::repeat_n('x', 10_000).collect();
            for _ in 0..10_000 {
                eprintln!("{s}");
            }
        })),
        Arc::new(SideEffect::new("fillstdout", || {
            let s: String = std::iter::repeat_n('x', 10_000).collect();
            for _ in 0..10_000 {
                println!("{s}");
            }
        })),
        Arc::new(SideEffect::new("divzero", || {
            #[allow(clippy::allow_attributes, unconditional_panic)]
            let val = { 1u8 / 0 };

            // simulate a side-effect via I/O so the compiler cannot optimize this method away
            println!("{val}");
        })),
        Arc::new(SideEffect::new("maxptr", || {
            let ptr = usize::MAX as *mut u8;

            // SAFETY: not safe ;)
            unsafe {
                *ptr = 42;
            }
        })),
        Arc::new(SideEffect::new("nullptr", || {
            // SAFETY: not safe ;)
            #[expect(deref_nullptr)]
            unsafe {
                *std::ptr::null_mut::<u8>() = 42;
            }
        })),
        Arc::new(SideEffect::new("panic", || panic!("foo"))),
        Arc::new(SideEffect::new("stackoverflow", || {
            // simulate a side-effect via I/O so the compiler cannot optimize this method away
            println!("{}", ackermann(10, 10));
        })),
    ])
}
