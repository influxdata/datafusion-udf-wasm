//! Payload that spins while retrieving the root filesystem.
use crate::spin::spin;

/// Return root file system.
///
/// This always returns [`None`] because the example does not need any files.
pub(crate) fn root() -> Option<Vec<u8>> {
    spin();
    None
}
