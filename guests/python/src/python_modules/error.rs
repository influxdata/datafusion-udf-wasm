//! Error handling helpers.
use pyo3::{exceptions::PyValueError, prelude::*};

/// Implements [`Display`](std::fmt::Display) by just forwarding it to [`Debug`](std::fmt::Debug).
macro_rules! display_like_debug {
    ($struct:ident) => {
        impl ::std::fmt::Display for $struct {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(self, f)
            }
        }
    };
}

pub(crate) use display_like_debug;

/// Wrapper that forwards [`Debug`](std::fmt::Debug) to [`Display`](std::fmt::Display).
pub(crate) struct DebugLikeDisplay<T>(pub(crate) T)
where
    T: std::fmt::Display;

impl<T> std::fmt::Debug for DebugLikeDisplay<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A resource (handle) was already used/moved/closed.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ResourceMoved;

display_like_debug!(ResourceMoved);

impl std::error::Error for ResourceMoved {}

impl From<ResourceMoved> for PyErr {
    fn from(e: ResourceMoved) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

/// Extensions trait for [`Option`] to simplify the work with [`ResourceMoved`].
pub(crate) trait ResourceMovedOptionExt {
    /// Option type.
    type T;

    /// Require the [`Option`] to be [`Some`], otherwise fail with [`ResourceMoved`].
    fn require_resource(self) -> Result<Self::T, ResourceMoved>;
}

impl<T> ResourceMovedOptionExt for Option<T> {
    type T = T;

    fn require_resource(self) -> Result<Self::T, ResourceMoved> {
        self.ok_or(ResourceMoved)
    }
}
