use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};

use futures_util::FutureExt;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

#[derive(Debug)]
struct Mock {
    result: Result<Vec<SocketAddr>, MockDnsError>,
    hits_expected: u64,
    hits_actual: u64,
}

#[derive(Debug, Default)]
struct State {
    mocks: HashMap<String, Mock>,
    errors: Vec<String>,
}

type SharedState = Arc<Mutex<State>>;

#[derive(Debug, Default, Clone)]
pub(crate) struct MockResolver {
    state: SharedState,
}

impl MockResolver {
    pub(crate) fn mock_ok(&self, name: impl ToString, addrs: Vec<SocketAddr>, hits: u64) {
        let name = name.to_string();

        let mut state = self.state.lock().unwrap();
        state.mocks.insert(
            name,
            Mock {
                result: Ok(addrs),
                hits_expected: hits,
                hits_actual: 0,
            },
        );
    }

    pub(crate) fn mock_err<E>(&self, name: impl ToString, e: E, hits: u64)
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let name = name.to_string();

        let mut state = self.state.lock().unwrap();
        state.mocks.insert(
            name,
            Mock {
                result: Err(MockDnsError { inner: Arc::new(e) }),
                hits_expected: hits,
                hits_actual: 0,
            },
        );
    }

    /// Clear mocks.
    ///
    /// # Panic
    /// Panics if a mock hit count doesn't match or there were any other errors.
    pub(crate) fn clear_mocks(&self) {
        let mut state = self.state.lock().unwrap();

        let mut errors = std::mem::take(&mut state.errors);

        // check hit rates
        for (name, mock) in state.mocks.drain() {
            if mock.hits_actual != mock.hits_expected {
                errors.push(format!(
                    "Should hit {expected} times but got {actual}:\n{name:#?}",
                    expected = mock.hits_expected,
                    actual = mock.hits_actual,
                ));
            }
        }

        drop(state);

        if !errors.is_empty() {
            let msg = format!("Resolver Errors:\n\n{}", errors.join("\n\n"));

            // don't double-panic
            if std::thread::panicking() {
                eprintln!("{msg}");
            } else {
                panic!("{msg}");
            }
        }
    }
}

impl Drop for MockResolver {
    fn drop(&mut self) {
        self.clear_mocks();
    }
}

impl Resolve for MockResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let mut state = self.state.lock().unwrap();
        let res = match state.mocks.get_mut(name.as_str()) {
            Some(mock) => {
                mock.hits_actual += 1;
                mock.result.clone()
            }
            None => {
                let msg = format!("no DNS mock for `{}`", name.as_str());
                state.errors.push(msg.clone());
                Err(MockDnsError {
                    inner: Arc::new(std::io::Error::other(msg)),
                })
            }
        };

        async move {
            match res {
                Ok(addrs) => Ok(Box::new(addrs.into_iter()) as Addrs),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
struct MockDnsError {
    inner: Arc<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for MockDnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DNS: {}", self.inner)
    }
}

impl std::error::Error for MockDnsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.inner.as_ref())
    }
}

/// There is no stable interface to construct a "DNS not found" error, so we need to create one.
pub(crate) fn dns_not_found_err() -> std::io::Error {
    // `.invalid` is reserved and will never resolve
    ("x.invalid", 0).to_socket_addrs().unwrap_err()
}

mod tests {
    use crate::integration_tests::python::runtime::http::test_utils::should_panic;

    use super::*;

    #[tokio::test]
    async fn test_happy_path() {
        let resolver = MockResolver::default();
        let addrs = vec!["127.0.0.1:0".parse().unwrap()];
        resolver.mock_ok("foo.test", addrs.clone(), 1);

        let actual = resolver
            .resolve("foo.test".parse().unwrap())
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(actual, addrs);
    }

    #[tokio::test]
    async fn test_not_hit() {
        let resolver = MockResolver::default();
        resolver.mock_ok("foo.test", vec![], 1);

        insta::assert_snapshot!(
            should_panic(move || {
                drop(resolver);
            }),
            @r#"
        Resolver Errors:

        Should hit 1 times but got 0:
        "foo.test"
        "#,
        );
    }

    #[tokio::test]
    async fn test_no_mock() {
        let resolver = MockResolver::default();

        // NOTE: cannot use `unwrap_err` here because the Ok(...) side doesn't implement Debug
        let err = match resolver.resolve("foo.test".parse().unwrap()).await {
            Ok(_) => {
                panic!("should not succeed")
            }
            Err(e) => e,
        };
        insta::assert_snapshot!(
            err.to_string(),
            @"DNS: no DNS mock for `foo.test`"
        );

        insta::assert_snapshot!(
            should_panic(move || {
                drop(resolver);
            }),
            @r"
        Resolver Errors:

        no DNS mock for `foo.test`
        ",
        );
    }

    #[tokio::test]
    async fn test_no_double_panic() {
        let resolver = MockResolver::default();
        resolver.mock_ok("foo.test", vec![], 1);

        insta::assert_snapshot!(
            should_panic(move || {
                if true {
                    panic!("foo");
                }
                drop(resolver);
            }),
            @"foo",
        );
    }
}
