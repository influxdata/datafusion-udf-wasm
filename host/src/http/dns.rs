//! DNS-related tools.
use std::{net::ToSocketAddrs, sync::Arc};

use rand::prelude::SliceRandom;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::task::JoinSet;

/// Dynamic error used by [`Resolve::resolve`].
type DynErr = Box<dyn std::error::Error + Send + Sync>;

/// DNS resolver that shuffles the response.
#[derive(Debug)]
pub(crate) struct ShuffleResolver;

impl Resolve for ShuffleResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            // use `JoinSet` to propagate cancellation to tasks that haven't started running yet.
            let mut tasks = JoinSet::new();
            tasks.spawn_blocking(move || {
                let it = (name.as_str(), 0).to_socket_addrs()?;
                let mut addrs = it.collect::<Vec<_>>();

                addrs.shuffle(&mut rand::rng());

                Ok(Box::new(addrs.into_iter()) as Addrs)
            });

            tasks
                .join_next()
                .await
                .expect("spawned on task")
                .map_err(|err| Box::new(err) as DynErr)?
        })
    }
}

/// DNS resolver that wraps a user-provided resolver and implements our application logic.
pub(crate) struct ResolverWrapper {
    /// User-provided resolver.
    inner: Arc<dyn Resolve>,
}

impl ResolverWrapper {
    /// Create new wrapper.
    pub(crate) fn new(inner: Arc<dyn Resolve>) -> Self {
        Self { inner }
    }
}

impl Resolve for ResolverWrapper {
    fn resolve(&self, name: Name) -> Resolving {
        let inner = Arc::clone(&self.inner);

        Box::pin(async move {
            let name_string = name.as_str().to_owned();
            let addrs = inner.resolve(name).await?.collect::<Vec<_>>();

            for addr in &addrs {
                if addr.port() != 0 {
                    return Err(Box::new(ResolvedPortNotZero {
                        name: name_string.clone(),
                        port: addr.port(),
                    }) as DynErr);
                }
            }

            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// A user-provided DNS resolver acquired an [`SocketAddr`](std::net::SocketAddr) with a non-zero port.
#[derive(Debug, Default)]
pub(crate) struct ResolvedPortNotZero {
    /// DNS name.
    name: String,

    /// Port.
    port: u16,
}

impl std::fmt::Display for ResolvedPortNotZero {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { name, port } = self;
        write!(f, "resolved port for `{name}` is not zero: {port}")
    }
}

impl std::error::Error for ResolvedPortNotZero {}
