use std::{
    collections::HashSet,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{net::TcpListener, task::JoinSet};
use wasmtime_wasi_http::DEFAULT_FORBIDDEN_HEADERS;

const LISTEN_ADDR_IPV4: &str = "127.0.0.1:0";
const LISTEN_ADDR_IPV6: &str = "[::1]:0";

pub(crate) type Request = http::Request<Bytes>;
pub(crate) type Response = http::Response<BoxBody<Bytes, Infallible>>;

#[derive(Debug, Default)]
pub(crate) struct Matcher {
    /// If not [`None`], assert that the method is one of the given ones.
    pub(crate) method: Option<HashSet<Method>>,

    /// If given, check the path.
    pub(crate) path: Option<String>,

    /// If given, check that each key-value pair in the header map is contained in the request.
    ///
    /// The request may contain extra headers.
    pub(crate) headers: Option<HeaderMap>,

    /// If given, check the content of the body.
    pub(crate) body: Option<String>,
}

impl Matcher {
    fn matches(&self, req: &Request) -> bool {
        let Self {
            method,
            path,
            headers,
            body,
        } = self;

        if let Some(methods) = method
            && !methods.contains(req.method())
        {
            return false;
        }

        if let Some(path) = path
            && path != req.uri().path()
        {
            return false;
        }

        if let Some(headers) = headers
            && !headers.keys().all(|k| {
                // Repeated headers may be comma-separated, or actually repeated. We need to deal with both.
                let has = req
                    .headers()
                    .get_all(k)
                    .iter()
                    .flat_map(|v| {
                        v.as_bytes().split(|b| {
                            char::from_u32(*b as _)
                                .map(|c| c == ',')
                                .unwrap_or_default()
                        })
                    })
                    .collect::<HashSet<_>>();

                headers
                    .get_all(k)
                    .iter()
                    .all(|v| has.contains(v.as_bytes()))
            })
        {
            return false;
        }

        if let Some(body) = body
            && body.as_bytes() != req.body().as_ref()
        {
            return false;
        }

        true
    }
}

pub(crate) trait ResponseGen: std::fmt::Debug + Send + Sync + 'static {
    fn resp(&self, req: &Request) -> Response;
}

#[derive(Debug, Default)]
pub(crate) struct SimpleResponseGen {
    pub(crate) status: StatusCode,
    pub(crate) headers: Option<HeaderMap>,
    pub(crate) body: String,
}

impl ResponseGen for SimpleResponseGen {
    fn resp(&self, _req: &Request) -> Response {
        let Self {
            status,
            headers,
            body,
        } = self;

        let mut builder = http::Response::builder().status(status);

        if let Some(headers) = headers {
            *builder.headers_mut().unwrap() = headers.clone();
        }

        builder
            .body(Full::new(Bytes::from(body.clone().into_bytes())).boxed())
            .expect("values provided to the builder should be valid")
    }
}

pub(crate) struct ResponseGenFn {
    f: Box<dyn for<'a> Fn(&'a Request) -> Response + Send + Sync>,
}

impl ResponseGenFn {
    pub(crate) fn new<F>(f: F) -> Self
    where
        F: for<'a> Fn(&'a Request) -> Response + Send + Sync + 'static,
    {
        Self { f: Box::new(f) }
    }
}

impl std::fmt::Debug for ResponseGenFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseGenFn").finish_non_exhaustive()
    }
}

impl ResponseGen for ResponseGenFn {
    fn resp(&self, req: &Request) -> Response {
        (self.f)(req)
    }
}

#[derive(Debug)]
pub(crate) struct ServerMock {
    pub(crate) matcher: Matcher,
    pub(crate) response: Box<dyn ResponseGen>,
    pub(crate) hits: Option<u64>,
}

impl Default for ServerMock {
    fn default() -> Self {
        Self {
            matcher: Default::default(),
            response: Box::new(SimpleResponseGen::default()),
            hits: Some(1),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    mocks: Vec<(ServerMock, u64)>,
    errors: Vec<String>,
}

impl State {
    #[must_use]
    fn fail(&mut self, msg: impl ToString) -> Response {
        let msg = msg.to_string();
        self.errors.push(msg.clone());
        http::Response::builder()
            .status(http::StatusCode::BAD_REQUEST)
            .body(Full::new(Bytes::from(msg.into_bytes())).boxed())
            .expect("values provided to the builder should be valid")
    }
}

#[derive(Debug, Default)]
pub(crate) enum IpVersion {
    #[default]
    V4,
    V6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// Reject all TCP connections.
    RejectConnections,

    /// Close connection w/o answering.
    CloseWithoutAnswer,
}

#[derive(Debug, Default)]
pub(crate) struct MockServerOptions {
    pub(crate) ip_version: IpVersion,
    pub(crate) failure: Option<Failure>,
}

type SharedState = Arc<Mutex<State>>;

#[derive(Debug)]
pub(crate) struct MockServer {
    _task: JoinSet<()>,
    state: SharedState,
    addr: SocketAddr,
}

impl MockServer {
    pub(crate) async fn start() -> Self {
        Self::with_options(MockServerOptions::default()).await
    }

    pub(crate) async fn with_options(options: MockServerOptions) -> Self {
        let MockServerOptions {
            ip_version,
            failure,
        } = options;
        let tcp_listener = TcpListener::bind(match ip_version {
            IpVersion::V4 => LISTEN_ADDR_IPV4,
            IpVersion::V6 => LISTEN_ADDR_IPV6,
        })
        .await
        .expect("bind");
        let addr = tcp_listener.local_addr().unwrap();

        let state = SharedState::default();

        let mut task = JoinSet::new();
        if failure != Some(Failure::RejectConnections) {
            let state_captured = Arc::clone(&state);
            task.spawn(async move {
                let mut connections = JoinSet::new();

                loop {
                    let (stream, accept_addr) = match tcp_listener.accept().await {
                        Ok(x) => x,
                        Err(e) => {
                            eprintln!("failed to accept connection: {e}");
                            continue;
                        }
                    };

                    if failure == Some(Failure::CloseWithoutAnswer) {
                        continue;
                    }

                    let state = Arc::clone(&state_captured);
                    let serve_connection = async move {
                        let result =
                            hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                                .serve_connection(
                                    TokioIo::new(stream),
                                    service_fn(move |req: http::Request<Incoming>| {
                                        let state = Arc::clone(&state);

                                        async move {
                                            // hydrate entire body so we can process it easier
                                            let (parts, body) = req.into_parts();
                                            let body = body.collect().await.unwrap().to_bytes();
                                            let req = http::Request::from_parts(parts, body);

                                            let mut state = state.lock().unwrap();

                                            if has_forbidden_headers(&req, addr) {
                                                return Ok(state.fail(format!(
                                                    "Forbidden headers:\n{req:#?}"
                                                )));
                                            }

                                            let Some((mock, count)) = state
                                                .mocks
                                                .iter_mut()
                                                .find(|(mock, _hits)| mock.matcher.matches(&req))
                                            else {
                                                return Ok(
                                                    state.fail(format!("Not mocked:\n{req:#?}"))
                                                );
                                            };

                                            *count += 1;

                                            let mut resp = mock.response.resp(&req);

                                            // combine repeated headers, but we should probably not do that
                                            // See https://github.com/influxdata/datafusion-udf-wasm/issues/452
                                            let headers = resp.headers_mut();
                                            *headers = headers
                                                .keys()
                                                .map(|k| {
                                                    let vals = headers
                                                        .get_all(k)
                                                        .iter()
                                                        .map(|v| v.to_str().unwrap())
                                                        .collect::<Vec<_>>();
                                                    let v = HeaderValue::from_str(&vals.join(","))
                                                        .unwrap();
                                                    (k.clone(), v)
                                                })
                                                .collect();

                                            Result::<_, Infallible>::Ok(resp)
                                        }
                                    }),
                                )
                                .await;

                        if let Err(e) = result {
                            eprintln!("error serving {accept_addr}: {e}");
                        }
                    };

                    connections.spawn(serve_connection);
                }
            });
        }

        Self {
            _task: task,
            state,
            addr,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub(crate) fn mock(&self, mock: ServerMock) {
        self.state.lock().unwrap().mocks.push((mock, 0));
    }

    /// Clear mocks.
    ///
    /// # Panic
    /// Panics if a mock hit count doesn't match or there were any other errors.
    pub(crate) fn clear_mocks(&self) {
        let mut state = self.state.lock().unwrap();

        let mut errors = std::mem::take(&mut state.errors);

        // check hit rates
        for (mock, hits) in state.mocks.drain(..) {
            let Some(expected) = mock.hits else {
                continue;
            };
            if hits != expected {
                errors.push(format!(
                    "Should hit {expected} times but got {hits}:\n{mock:#?}",
                ));
            }
        }

        drop(state);

        if !errors.is_empty() {
            let msg = format!("Errors:\n\n{}", errors.join("\n\n"));

            // don't double-panic
            if std::thread::panicking() {
                eprintln!("{msg}");
            } else {
                panic!("{msg}");
            }
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.clear_mocks();
    }
}

fn has_forbidden_headers(request: &Request, addr: SocketAddr) -> bool {
    // "host" is part of the forbidden headers that the client is not supposed to use, but it is set by our own
    // host HTTP lib
    if let Some(host_val) = request.headers().get(http::header::HOST)
        && host_val.to_str().expect("always a string") != addr.to_string()
    {
        return true;
    }

    DEFAULT_FORBIDDEN_HEADERS
        .iter()
        .filter(|h| *h != http::header::HOST)
        .any(|h| request.headers().contains_key(h))
}

/// Test the tester.
mod tests {
    use std::sync::LazyLock;

    use http::HeaderValue;
    use regex::Regex;

    use super::*;

    #[test]
    fn test_matcher() {
        // simple
        assert!(Matcher::default().matches(&http::Request::builder().body(Bytes::new()).unwrap()));

        // method
        assert!(
            Matcher::default().matches(
                &http::Request::builder()
                    .method(http::Method::GET)
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            Matcher::default().matches(
                &http::Request::builder()
                    .method(http::Method::POST)
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            !Matcher {
                method: Some(HashSet::from([http::Method::POST])),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .method(http::Method::GET)
                    .body(Bytes::new())
                    .unwrap()
            )
        );

        // path
        assert!(
            Matcher::default().matches(
                &http::Request::builder()
                    .uri("http://foo.bar/x")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            Matcher {
                path: Some("/x".to_owned()),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .uri("http://foo.bar/x")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            !Matcher {
                path: Some("/x/y".to_owned()),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .uri("http://foo.bar/x")
                    .body(Bytes::new())
                    .unwrap()
            )
        );

        // headers
        assert!(
            Matcher::default().matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "*")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            Matcher {
                headers: Some(
                    [(http::header::ACCEPT, HeaderValue::from_str("foo").unwrap())]
                        .into_iter()
                        .collect()
                ),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "foo")
                    .header(http::header::ACCEPT_RANGES, "bar")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            !Matcher {
                headers: Some(
                    [
                        (http::header::ACCEPT, HeaderValue::from_str("foo").unwrap()),
                        (http::header::ACCEPT, HeaderValue::from_str("bar").unwrap())
                    ]
                    .into_iter()
                    .collect()
                ),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "foo")
                    .header(http::header::ACCEPT, "baz")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            Matcher {
                headers: Some(
                    [
                        (http::header::ACCEPT, HeaderValue::from_str("foo").unwrap()),
                        (http::header::ACCEPT, HeaderValue::from_str("bar").unwrap())
                    ]
                    .into_iter()
                    .collect()
                ),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "foo")
                    .header(http::header::ACCEPT, "bar")
                    .header(http::header::ACCEPT, "baz")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            !Matcher {
                headers: Some(
                    [
                        (http::header::ACCEPT, HeaderValue::from_str("foo").unwrap()),
                        (http::header::ACCEPT, HeaderValue::from_str("bar").unwrap())
                    ]
                    .into_iter()
                    .collect()
                ),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "foo,baz")
                    .body(Bytes::new())
                    .unwrap()
            )
        );
        assert!(
            Matcher {
                headers: Some(
                    [
                        (http::header::ACCEPT, HeaderValue::from_str("foo").unwrap()),
                        (http::header::ACCEPT, HeaderValue::from_str("bar").unwrap())
                    ]
                    .into_iter()
                    .collect()
                ),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .header(http::header::ACCEPT, "foo,bar,baz")
                    .body(Bytes::new())
                    .unwrap()
            )
        );

        // body
        assert!(
            Matcher::default().matches(
                &http::Request::builder()
                    .body(Bytes::from_static(b"foo"))
                    .unwrap()
            )
        );
        assert!(
            Matcher {
                body: Some("foo".to_owned()),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .body(Bytes::from_static(b"foo"))
                    .unwrap()
            )
        );
        assert!(
            !Matcher {
                body: Some("foo".to_owned()),
                ..Default::default()
            }
            .matches(
                &http::Request::builder()
                    .body(Bytes::from_static(b"bar"))
                    .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn test_default_works() {
        let server = MockServer::start().await;
        server.mock(ServerMock::default());

        let resp = reqwest::Client::new()
            .get(server.uri())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "");
    }

    #[tokio::test]
    async fn test_not_hit() {
        let server = MockServer::start().await;
        server.mock(ServerMock::default());

        insta::assert_snapshot!(
            should_panic(move || {
                drop(server);
            }),
            @r#"
        Errors:

        Should hit 1 times but got 0:
        ServerMock {
            matcher: Matcher {
                method: None,
                path: None,
                headers: None,
                body: None,
            },
            response: SimpleResponseGen {
                status: 200,
                headers: None,
                body: "",
            },
            hits: Some(
                1,
            ),
        }
        "#,
        );
    }

    #[tokio::test]
    async fn test_no_double_panic() {
        let server = MockServer::start().await;
        server.mock(ServerMock::default());

        insta::assert_snapshot!(
            should_panic(move || {
                if true {
                    panic!("foo");
                }
                drop(server);
            }),
            @"foo",
        );
    }

    #[tokio::test]
    async fn test_forbidden_headers() {
        let server = MockServer::start().await;

        let resp = reqwest::Client::new()
            .get(server.uri())
            .header(http::header::CONNECTION, "upgrade")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        insta::assert_snapshot!(
            normalize_addr(resp.text().await.unwrap()),
            @r#"
        Forbidden headers:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "connection": "upgrade",
                "accept": "*/*",
                "host": "<ADDR>",
            },
            body: b"",
        }
        "#,
        );

        insta::assert_snapshot!(
            normalize_addr(should_panic(move || {
                drop(server);
            })),
            @r#"
        Errors:

        Forbidden headers:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "connection": "upgrade",
                "accept": "*/*",
                "host": "<ADDR>",
            },
            body: b"",
        }
        "#,
        );
    }

    #[tokio::test]
    async fn test_wrong_hostname() {
        let server = MockServer::start().await;

        let resp = reqwest::Client::new()
            .get(server.uri())
            .header(http::header::HOST, "foo.bar")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        insta::assert_snapshot!(
            normalize_addr(resp.text().await.unwrap()),
            @r#"
        Forbidden headers:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "host": "foo.bar",
                "accept": "*/*",
            },
            body: b"",
        }
        "#,
        );

        insta::assert_snapshot!(
            normalize_addr(should_panic(move || {
                drop(server);
            })),
            @r#"
        Errors:

        Forbidden headers:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "host": "foo.bar",
                "accept": "*/*",
            },
            body: b"",
        }
        "#,
        );
    }

    #[tokio::test]
    async fn test_not_mocked() {
        let server = MockServer::start().await;

        let resp = reqwest::Client::new()
            .get(server.uri())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        insta::assert_snapshot!(
            normalize_addr(resp.text().await.unwrap()),
            @r#"
        Not mocked:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "accept": "*/*",
                "host": "<ADDR>",
            },
            body: b"",
        }
        "#,
        );

        insta::assert_snapshot!(
            normalize_addr(should_panic(move || {
                drop(server);
            })),
            @r#"
        Errors:

        Not mocked:
        Request {
            method: GET,
            uri: /,
            version: HTTP/1.1,
            headers: {
                "accept": "*/*",
                "host": "<ADDR>",
            },
            body: b"",
        }
        "#,
        );
    }

    fn should_panic<F>(f: F) -> String
    where
        F: FnOnce(),
    {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(()) => panic!("did not panic"),
            Err(msg) => {
                if let Some(msg) = msg.downcast_ref::<&str>() {
                    msg.to_string()
                } else if let Some(msg) = msg.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    panic!("cannot extract message")
                }
            }
        }
    }

    /// Normalize addresses since they are not deterministic.
    fn normalize_addr(e: impl ToString) -> String {
        let e = e.to_string();

        static REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#"[0-9]+(\.[0-9]+){3}:[0-9]+"#).unwrap());

        REGEX.replace_all(&e, r#"<ADDR>"#).to_string()
    }
}
