//! Defines a few missing symbols.
//!
//! See <https://github.com/bytecodealliance/componentize-py/blob/f340fb56f45213342ce400de95b2ae1f616ff7f7/runtime/src/lib.rs#L2169-L2198>
use std::alloc::{Layout, alloc};

/// Constant for linking.
#[unsafe(no_mangle)]
static _CLOCK_PROCESS_CPUTIME_ID: u8 = 2;

/// Constant for linking.
#[unsafe(no_mangle)]
static _CLOCK_THREAD_CPUTIME_ID: u8 = 3;

/// Traditionally, `wit-bindgen` would provide a `cabi_realloc` implementation,
/// but recent versions use a weak symbol trick to avoid conflicts when more than
/// one `wit-bindgen` version is used, and that trick does not currently play
/// nice with how we build this library.  So for now, we just define it ourselves
/// here:
#[unsafe(export_name = "cabi_realloc")]
unsafe extern "C" fn cabi_realloc(
    old_ptr: *mut u8,
    old_len: usize,
    align: usize,
    new_size: usize,
) -> *mut u8 {
    assert!(old_ptr.is_null());
    assert!(old_len == 0);

    // SAFETY: this just emulates `realloc` using `alloc`
    unsafe { alloc(Layout::from_size_align(new_size, align).unwrap()) }
}
