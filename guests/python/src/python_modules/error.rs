//! Error handling helpers.
use pyo3::{exceptions::PyValueError, prelude::*};

/// A resource (handle) was already used/moved/closed.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ResourceMoved;

impl std::fmt::Display for ResourceMoved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResourceMoved")
    }
}

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
