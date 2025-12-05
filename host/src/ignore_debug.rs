//! Helper to simplify [`Debug`] implementation by ignoring it.

use std::ops::{Deref, DerefMut};

/// Helper to simplify [`Debug`] implementation by ignoring it.
pub(crate) struct IgnoreDebug<T>(T);

impl<T> std::fmt::Debug for IgnoreDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}>", std::any::type_name::<T>())
    }
}

impl<T> Deref for IgnoreDebug<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for IgnoreDebug<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for IgnoreDebug<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_debug() {
        assert_eq!(format!("{:?}", IgnoreDebug::from(1u8)), "<u8>");
        assert_eq!(
            format!("{:?}", IgnoreDebug::from(Arc::<str>::from("foo"))),
            "<alloc::sync::Arc<str>>",
        );
    }
}
