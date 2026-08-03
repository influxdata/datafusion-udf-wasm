//! DNS-related tools.
use std::{net::ToSocketAddrs, sync::Arc};

use rand::prelude::SliceRandom;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::task::JoinSet;

use crate::http::HttpRequestValidator;

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

    /// HTTP request validator.
    validator: Arc<dyn HttpRequestValidator>,
}

impl ResolverWrapper {
    /// Create new wrapper.
    pub(crate) fn new(inner: Arc<dyn Resolve>, validator: Arc<dyn HttpRequestValidator>) -> Self {
        Self { inner, validator }
    }
}

impl Resolve for ResolverWrapper {
    fn resolve(&self, name: Name) -> Resolving {
        let inner = Arc::clone(&self.inner);
        let validator = Arc::clone(&self.validator);

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

            let allowed_addrs = addrs
                .iter()
                .copied()
                .filter(|addr| validator.validate_ip(&name_string, addr.ip()).is_ok())
                .collect::<Vec<_>>();

            if allowed_addrs.is_empty() && !addrs.is_empty() {
                return Err(Box::new(ResolvedIpRejected { name: name_string }) as DynErr);
            }

            Ok(Box::new(allowed_addrs.into_iter()) as Addrs)
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

/// All IP addresses returned for a host were rejected by the HTTP request validator.
#[derive(Debug)]
pub(crate) struct ResolvedIpRejected {
    /// DNS name.
    name: String,
}

impl std::fmt::Display for ResolvedIpRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "all resolved IPs for `{}` were rejected", self.name)
    }
}

impl std::error::Error for ResolvedIpRejected {}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use crate::{AllowCertainHttpRequests, HttpRequestValidator};

    use super::*;

    #[derive(Debug)]
    struct StaticResolver(Vec<SocketAddr>);

    impl Resolve for StaticResolver {
        fn resolve(&self, _name: Name) -> Resolving {
            let addrs = self.0.clone();
            Box::pin(async move { Ok(Box::new(addrs.into_iter()) as Addrs) })
        }
    }

    #[tokio::test]
    async fn test_filters_rejected_ips() {
        let mut validator = AllowCertainHttpRequests::new();
        validator
            .allow_host("example.test")
            .deny_subnet("127.0.0.0/8".parse().unwrap());
        let resolver = ResolverWrapper::new(
            Arc::new(StaticResolver(vec![
                "127.0.0.1:0".parse().unwrap(),
                "[::1]:0".parse().unwrap(),
            ])),
            Arc::new(validator),
        );

        let addrs = resolver
            .resolve("example.test".parse().unwrap())
            .await
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(addrs, vec!["[::1]:0".parse::<SocketAddr>().unwrap()]);
    }

    #[tokio::test]
    async fn test_rejects_all_ips() {
        let mut validator = AllowCertainHttpRequests::new();
        validator
            .allow_host("example.test")
            .deny_subnet("127.0.0.0/8".parse().unwrap());
        let resolver = ResolverWrapper::new(
            Arc::new(StaticResolver(vec!["127.0.0.1:0".parse().unwrap()])),
            Arc::new(validator),
        );

        let err = match resolver.resolve("example.test".parse().unwrap()).await {
            Ok(_) => panic!("all IPs should be rejected"),
            Err(err) => err,
        };

        assert!(err.downcast_ref::<ResolvedIpRejected>().is_some());
    }

    #[test]
    fn test_default_ip_validation_allows_ips() {
        let validator = crate::RejectAllHttpRequests;
        assert!(
            validator
                .validate_ip("example.test", "127.0.0.1".parse().unwrap())
                .is_ok()
        );
    }
}
