//! Payloads that lock the CPU during various operations.

use std::hash::{DefaultHasher, Hash, Hasher};

pub(crate) mod root;
pub(crate) mod udf_invoke;
pub(crate) mod udf_name;
pub(crate) mod udf_return_type_exact;
pub(crate) mod udf_return_type_other;
pub(crate) mod udf_signature;
pub(crate) mod udfs;

/// Method that locks the CPU forever.
fn spin() {
    // Use a non-predictable costly loop.
    //
    // Practically `u64::MAX` will never come to an end.
    let mut hasher = DefaultHasher::new();
    for i in 0..u64::MAX {
        i.hash(&mut hasher);
    }
    let result = hasher.finish();

    // side-effect to prevent the compiler from removing our code
    println!("hash: {result}");
}
