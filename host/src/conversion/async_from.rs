//! Async conversation traits.

/// Async version of [`TryFrom`].
pub(crate) trait AsyncTryFrom<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion.
    async fn async_try_from(value: T) -> Result<Self, Self::Error>;
}

/// Async version of [`TryInto`].
pub(crate) trait AsyncTryInto<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion.
    async fn async_try_into(self) -> Result<T, Self::Error>;
}

impl<T, U> AsyncTryInto<U> for T
where
    U: AsyncTryFrom<T>,
{
    type Error = U::Error;

    async fn async_try_into(self) -> Result<U, U::Error> {
        U::async_try_from(self).await
    }
}
