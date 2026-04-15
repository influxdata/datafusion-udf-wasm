//! [`AllowHttpEndpoint`].
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    str::FromStr,
};

use datafusion_common::{DataFusionError, Result as DataFusionResult, config::ConfigField};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use crate::{
    error::DataFusionResultExt,
    http::{
        types::{HttpConnectionMode, HttpMethod, HttpPort},
        validator::{HttpRequestRejected, HttpRequestValidator},
    },
};

/// Allow settings for a given endpoint.
///
/// An endpoint is defined by a host + port.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowHttpEndpoint {
    /// Connection mode.
    mode: HttpConnectionMode,

    /// Allowed methods.
    methods: HashSet<HttpMethod>,
}

impl AllowHttpEndpoint {
    /// Separator for methods.
    const METHOD_SEP: &str = "|";

    /// Allow given connection mode.
    ///
    /// Note that only one mode can be allowed. Calling this method multiple times will keep the last value.
    pub fn allow_mode(&mut self, mode: HttpConnectionMode) {
        self.mode = mode;
    }

    /// Allow given HTTP method.
    ///
    /// Multiple methods can be allowed.
    pub fn allow_method(&mut self, method: HttpMethod) {
        self.methods.insert(method);
    }
}

impl ConfigField for AllowHttpEndpoint {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        _description: &'static str,
    ) {
        let Self { mode, methods } = self;

        v.some(&format!("{key}.mode"), mode, "HTTP connection mode");

        let mut methods = methods.iter().map(|m| m.to_string()).collect::<Vec<_>>();
        methods.sort_unstable();
        v.some(
            &format!("{key}.methods"),
            methods.join(Self::METHOD_SEP),
            "HTTP method",
        );
    }

    fn set(&mut self, key: &str, value: &str) -> DataFusionResult<()> {
        match key {
            "mode" => {
                let mode: HttpConnectionMode = value.parse().map_err(|e| {
                    DataFusionError::External(Box::new(e))
                        .context("cannot parse HTTP connection mode")
                })?;
                self.mode = mode;
                Ok(())
            }
            "methods" => {
                let methods = value
                    .split(Self::METHOD_SEP)
                    .map(|s| {
                        HttpMethod::from_str(s).map_err(|e| {
                            DataFusionError::External(Box::new(e))
                                .context("cannot parse HTTP method")
                        })
                    })
                    .collect::<Result<HashSet<_>, _>>()?;
                self.methods = methods;
                Ok(())
            }
            other => Err(DataFusionError::Configuration(format!(
                "unknown field: `{other}`"
            ))),
        }
    }
}

/// Allow settings for a host.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowHttpHost {
    /// Mapping from port to endpoint.
    ports: HashMap<HttpPort, AllowHttpEndpoint>,
}

impl AllowHttpHost {
    /// Allow given port at this host.
    pub fn allow_port(&mut self, port: HttpPort) -> &mut AllowHttpEndpoint {
        self.ports.entry(port).or_default()
    }
}

impl ConfigField for AllowHttpHost {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        _description: &'static str,
    ) {
        let Self { ports } = self;

        let mut ports = ports.iter().collect::<Vec<_>>();
        ports.sort_unstable_by_key(|(port, _cfg)| *port);

        for (port, cfg) in ports {
            let key = format!("{key}.port.{port}");
            cfg.visit(v, &key, "");
        }
    }

    fn set(&mut self, key: &str, value: &str) -> DataFusionResult<()> {
        let (field, key) = key.split_once(".").unwrap_or((key, ""));

        match field {
            "port" => {
                let (port, key) = key.split_once(".").ok_or_else(|| {
                    DataFusionError::Configuration(format!(
                        "port must be terminated by `.`: `{key}`"
                    ))
                })?;
                let port: HttpPort = port
                    .parse()
                    .map_err(|e| DataFusionError::External(Box::new(e)).context("parse port"))?;
                self.allow_port(port)
                    .set(key, value)
                    .context("parse port config")
            }
            other => Err(DataFusionError::Configuration(format!(
                "unknown field: `{other}`"
            ))),
        }
    }
}

/// Allow-list requests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowCertainHttpRequests {
    /// Set of allowed hosts.
    hosts: HashMap<Cow<'static, str>, AllowHttpHost>,
}

impl AllowCertainHttpRequests {
    /// Create new, empty request matcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow given host.
    pub fn allow_host(&mut self, host: impl Into<Cow<'static, str>>) -> &mut AllowHttpHost {
        self.hosts.entry(host.into()).or_default()
    }
}

impl HttpRequestValidator for AllowCertainHttpRequests {
    fn validate(
        &self,
        request: &hyper::Request<HyperOutgoingBody>,
        mode: HttpConnectionMode,
    ) -> Result<(), HttpRequestRejected> {
        let host = self
            .hosts
            .get(request.uri().host().ok_or(HttpRequestRejected)?)
            .ok_or(HttpRequestRejected)?;

        let endpoint = host
            .ports
            .get(
                &request
                    .uri()
                    .port_u16()
                    .map(|p| HttpPort::new(p).ok_or(HttpRequestRejected))
                    .transpose()?
                    .unwrap_or_else(|| mode.default_port()),
            )
            .ok_or(HttpRequestRejected)?;

        if endpoint.mode != mode {
            return Err(HttpRequestRejected);
        }

        if !endpoint.methods.contains(request.method()) {
            return Err(HttpRequestRejected);
        }

        Ok(())
    }
}

impl ConfigField for AllowCertainHttpRequests {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        _description: &'static str,
    ) {
        let Self { hosts } = self;

        let mut hosts = hosts.iter().collect::<Vec<_>>();
        hosts.sort_unstable_by_key(|(host, _cfg)| *host);

        for (host, cfg) in hosts {
            let key = format!("{key}.host.[{host}]");
            cfg.visit(v, &key, "");
        }
    }

    fn set(&mut self, key: &str, value: &str) -> DataFusionResult<()> {
        let (field, key) = key.split_once(".").unwrap_or((key, ""));

        match field {
            "host" => {
                let (host, key) = key
                    .strip_prefix("[")
                    .and_then(|s| s.split_once("]."))
                    .ok_or_else(|| {
                        DataFusionError::Configuration(format!(
                            "host must be surrounded by `.[` and `].`: `{key}`"
                        ))
                    })?;
                self.allow_host(host.to_owned())
                    .set(key, value)
                    .context("parse host config")
            }
            other => Err(DataFusionError::Configuration(format!(
                "unknown field: `{other}`"
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use std::fmt::{Display, Write};

    use datafusion_common::config::ConfigEntry;

    use super::*;

    #[test]
    fn test_allow_deny() {
        const HOST_1: &str = "foo.bar";
        const HOST_2: &str = "my.universe";
        const SPECIFIC_PORT: HttpPort = HttpPort::new(1337).expect("valid port");

        let request_no_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_1}"))
            .body(Default::default())
            .unwrap();

        let request_with_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_2}:{SPECIFIC_PORT}"))
            .body(Default::default())
            .unwrap();

        let request_zero_port = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri(format!("http://{HOST_1}:0"))
            .body(Default::default())
            .unwrap();

        #[derive(Debug, PartialEq, Eq)]
        struct Results {
            no_port_no_tls: Result<(), HttpRequestRejected>,
            no_port_with_tls: Result<(), HttpRequestRejected>,
            with_port_no_tls: Result<(), HttpRequestRejected>,
            with_port_with_tls: Result<(), HttpRequestRejected>,
        }

        #[derive(Debug)]
        struct Case {
            policy: AllowCertainHttpRequests,
            results: Results,
        }

        let cases = [
            Case {
                policy: AllowCertainHttpRequests::default(),
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy.allow_host(HOST_1);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port());
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port())
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port());
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Ok(()),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::Encrypted.default_port())
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Ok(()),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::Encrypted.default_port());
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port())
                        .allow_method(HttpMethod::POST);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_2)
                        .allow_port(HttpConnectionMode::PlainText.default_port())
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();
                    policy
                        .allow_host(HOST_2)
                        .allow_port(SPECIFIC_PORT)
                        .allow_method(HttpMethod::GET);
                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Ok(()),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint = policy.allow_host(HOST_2).allow_port(SPECIFIC_PORT);
                    endpoint.allow_mode(HttpConnectionMode::PlainText);
                    endpoint.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Err(HttpRequestRejected),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Ok(()),
                    with_port_with_tls: Err(HttpRequestRejected),
                },
            },
            Case {
                policy: {
                    let mut policy = AllowCertainHttpRequests::default();

                    let endpoint_1 = policy
                        .allow_host(HOST_1)
                        .allow_port(HttpConnectionMode::PlainText.default_port());
                    endpoint_1.allow_mode(HttpConnectionMode::PlainText);
                    endpoint_1.allow_method(HttpMethod::GET);
                    endpoint_1.allow_method(HttpMethod::POST);

                    let endpoint_2 = policy.allow_host(HOST_2).allow_port(SPECIFIC_PORT);
                    endpoint_2.allow_method(HttpMethod::GET);

                    policy
                },
                results: Results {
                    no_port_no_tls: Ok(()),
                    no_port_with_tls: Err(HttpRequestRejected),
                    with_port_no_tls: Err(HttpRequestRejected),
                    with_port_with_tls: Ok(()),
                },
            },
        ];

        for (i, case) in cases.into_iter().enumerate() {
            println!("========================================");
            println!("case #{}:", i + 1);
            println!("{case:#?}");

            let Case {
                policy,
                results: results_expected,
            } = case;

            let results_actual = Results {
                no_port_no_tls: policy.validate(&request_no_port, HttpConnectionMode::PlainText),
                no_port_with_tls: policy.validate(&request_no_port, HttpConnectionMode::Encrypted),
                with_port_no_tls: policy
                    .validate(&request_with_port, HttpConnectionMode::PlainText),
                with_port_with_tls: policy
                    .validate(&request_with_port, HttpConnectionMode::Encrypted),
            };

            assert!(
                results_actual == results_expected,
                "\nActual:\n{results_actual:#?}",
            );

            // zero port is never allowed
            policy
                .validate(&request_zero_port, HttpConnectionMode::PlainText)
                .unwrap_err();
            policy
                .validate(&request_zero_port, HttpConnectionMode::Encrypted)
                .unwrap_err();
        }
    }

    #[test]
    fn test_config_parsing_ok() {
        let cfg = AllowCertainHttpRequests::default();
        insta::assert_snapshot!(
            config_roundtrip(cfg),
            @"",
        );

        let mut cfg = AllowCertainHttpRequests::default();
        cfg.allow_host("foo.bar")
            .allow_port(HttpPort::new(1337).unwrap())
            .allow_method(HttpMethod::POST);
        insta::assert_snapshot!(
            config_roundtrip(cfg),
            @r"
        # HTTP connection mode
        test.host.[foo.bar].port.1337.mode=encrypted

        # HTTP method
        test.host.[foo.bar].port.1337.methods=POST
        ",
        );

        let mut cfg = AllowCertainHttpRequests::default();
        let host_1 = cfg.allow_host("foo.bar");
        let endpoint_1_1 = host_1.allow_port(HttpPort::new(1337).unwrap());
        endpoint_1_1.allow_mode(HttpConnectionMode::PlainText);
        endpoint_1_1.allow_method(HttpMethod::POST);
        endpoint_1_1.allow_method(HttpMethod::GET);
        let endpoint_1_2 = host_1.allow_port(HttpPort::new(42).unwrap());
        endpoint_1_2.allow_method(HttpMethod::PATCH);
        let host_2 = cfg.allow_host("my.com");
        let endpoint_2_1 = host_2.allow_port(HttpPort::new(1337).unwrap());
        endpoint_2_1.allow_method(HttpMethod::GET);
        insta::assert_snapshot!(
            config_roundtrip(cfg),
            @r"
        # HTTP connection mode
        test.host.[foo.bar].port.42.mode=encrypted

        # HTTP method
        test.host.[foo.bar].port.42.methods=PATCH

        # HTTP connection mode
        test.host.[foo.bar].port.1337.mode=plaintext

        # HTTP method
        test.host.[foo.bar].port.1337.methods=GET|POST

        # HTTP connection mode
        test.host.[my.com].port.1337.mode=encrypted

        # HTTP method
        test.host.[my.com].port.1337.methods=GET
        ",
        );

        let mut cfg = AllowCertainHttpRequests::default();
        cfg.allow_host("127.0.0.1")
            .allow_port(HttpPort::new(1337).unwrap())
            .allow_method(HttpMethod::POST);
        cfg.allow_host("::1")
            .allow_port(HttpPort::new(1337).unwrap())
            .allow_method(HttpMethod::POST);
        insta::assert_snapshot!(
            config_roundtrip(cfg),
            @r"
        # HTTP connection mode
        test.host.[127.0.0.1].port.1337.mode=encrypted

        # HTTP method
        test.host.[127.0.0.1].port.1337.methods=POST

        # HTTP connection mode
        test.host.[::1].port.1337.mode=encrypted

        # HTTP method
        test.host.[::1].port.1337.methods=POST
        ",
        );
    }

    #[test]
    fn test_config_parsing_err() {
        insta::assert_snapshot!(
            config_parsing_err("test.no_such_field=1"),
            @"Invalid or Unsupported Configuration: unknown field: `no_such_field`",
        );
        insta::assert_snapshot!(
            config_parsing_err("test.host.foo.port.1337.methods=GET"),
            @"Invalid or Unsupported Configuration: host must be surrounded by `.[` and `].`: `foo.port.1337.methods`",
        );
        insta::assert_snapshot!(
            config_parsing_err("test.host.[foo].port.x.methods=GET"),
            @r"
        parse host config
        caused by
        parse port
        caused by
        External error: invalid digit found in string
        ",
        );
        insta::assert_snapshot!(
            config_parsing_err("test.host.[foo].port.1337.mode=foo"),
            @r"
        parse host config
        caused by
        parse port config
        caused by
        cannot parse HTTP connection mode
        caused by
        External error: Invalid HTTP connection mode: `foo`
        ",
        );
    }

    fn try_config_parsing(txt: &str) -> DataFusionResult<AllowCertainHttpRequests> {
        let mut cfg = AllowCertainHttpRequests::default();
        for line in txt.lines() {
            let line = line.trim();
            // skip comment / description & empty lines
            if line.starts_with("#") || line.is_empty() {
                continue;
            }
            let (k, v) = line.split_once("=").unwrap();
            let k = k.strip_prefix("test.").unwrap();
            cfg.set(k, v)?;
        }
        Ok(cfg)
    }

    #[track_caller]
    fn config_parsing_err(txt: &str) -> DataFusionError {
        try_config_parsing(txt).unwrap_err()
    }

    #[track_caller]
    fn config_roundtrip(expected: AllowCertainHttpRequests) -> String {
        let txt = config_entries_to_txt(&ConfigEntriesCollector::collect(&expected));
        let actual = match try_config_parsing(&txt) {
            Ok(actual) => actual,
            Err(err) => panic!("cannot parse config txt:\n\nErr:\n{err}\n\nText:\n{txt}"),
        };
        assert_eq!(actual, expected);
        txt
    }

    struct ConfigEntriesCollector(Vec<ConfigEntry>);

    impl ConfigEntriesCollector {
        fn collect(cfg: &AllowCertainHttpRequests) -> Vec<ConfigEntry> {
            let mut v = Self(vec![]);
            cfg.visit(&mut v, "test", "");
            v.0
        }
    }

    impl datafusion_common::config::Visit for ConfigEntriesCollector {
        fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
            self.0.push(ConfigEntry {
                key: key.to_string(),
                value: Some(value.to_string()),
                description,
            });
        }

        fn none(&mut self, key: &str, description: &'static str) {
            self.0.push(ConfigEntry {
                key: key.to_string(),
                value: None,
                description,
            });
        }
    }

    fn config_entries_to_txt(entries: &[ConfigEntry]) -> String {
        let mut out = String::new();

        for (i, entry) in entries.iter().enumerate() {
            if i > 0 {
                writeln!(&mut out).unwrap();
            }

            let ConfigEntry {
                key,
                value,
                description,
            } = entry;
            writeln!(&mut out, "# {description}").unwrap();

            if let Some(value) = value {
                writeln!(&mut out, "{key}={value}").unwrap();
            } else {
                writeln!(&mut out, "{key}").unwrap();
            }
        }

        out
    }
}
