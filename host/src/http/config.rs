//! Config for HTTP integration.

use std::sync::Arc;

use reqwest::dns::Resolve;

use crate::{HttpRequestValidator, RejectAllHttpRequests, http::dns::ShuffleResolver};

/// HTTP-related configs.
#[derive(Clone)]
pub struct HttpConfig {
    /// Maximum idle connection per host allowed in the pool.
    pub(crate) pool_max_idle_per_host: usize,

    /// DNS resolver.
    pub(crate) resolver: Arc<dyn Resolve>,

    /// Validator.
    pub(crate) validator: Arc<dyn HttpRequestValidator>,
}

impl HttpConfig {
    /// Sets the maximum idle connection per host allowed in the pool.
    ///
    /// # Default
    /// Default is `usize::MAX` (no limit).
    pub fn with_pool_max_idle_per_host(self, max: usize) -> Self {
        Self {
            pool_max_idle_per_host: max,
            ..self
        }
    }

    /// Set DNS resolver.
    ///
    /// # Implementation
    /// You may provide any implementation you want, however you MUST make sure that [`Resolve::resolve`] only returns
    /// [`SocketAddr`](std::net::SocketAddr) with the port set to `0`. Not complying with this requirement will lead to
    /// "internal" WASI errors.
    ///
    /// # Default
    /// The default is a resolver that uses the operating system (via `libc`) and shuffles the addresses before
    /// connecting (for better load balancing).
    pub fn with_resolver<R>(self, resolver: R) -> Self
    where
        R: Resolve + 'static,
    {
        Self {
            resolver: Arc::new(resolver),
            ..self
        }
    }

    /// Set HTTP validator.
    ///
    /// # Default
    /// The default is set to ["reject all"](RejectAllHttpRequests).
    pub fn with_validator<V>(self, validator: V) -> Self
    where
        V: HttpRequestValidator,
    {
        Self {
            validator: Arc::new(validator),
            ..self
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            resolver: Arc::new(ShuffleResolver),
            pool_max_idle_per_host: usize::MAX,
            validator: Arc::new(RejectAllHttpRequests),
        }
    }
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            pool_max_idle_per_host,
            // doesn't implement Debug
            resolver: _,
            validator,
        } = self;

        f.debug_struct("HttpConfig")
            .field("pool_max_idle_per_host", pool_max_idle_per_host)
            .field("resolver", &"<RESOLVER>")
            .field("validator", validator)
            .finish()
    }
}
