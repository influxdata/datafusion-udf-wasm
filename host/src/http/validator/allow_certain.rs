//! [`AllowHttpEndpoint`].
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet},
    net::IpAddr,
    str::FromStr,
};

use datafusion_common::{DataFusionError, Result as DataFusionResult, config::ConfigField};
use ipnet::IpNet;
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
///
/// A resolved IP address is allowed if it is contained in the allow-list, or the allow-list is empty, and it is not
/// contained in the deny-list. A deny-list entry always takes precedence over an allow-list entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowHttpHost {
    /// Mapping from port to endpoint.
    ports: HashMap<HttpPort, AllowHttpEndpoint>,

    /// Allow-listed IP subnets.
    allow_subnets: BTreeSet<IpNet>,

    /// Deny-listed IP subnets.
    deny_subnets: BTreeSet<IpNet>,
}

impl AllowHttpHost {
    /// Separator for IP subnets in the DataFusion configuration.
    const SUBNET_SEP: &str = "|";

    /// Allow given port at this host.
    pub fn allow_port(&mut self, port: HttpPort) -> &mut AllowHttpEndpoint {
        self.ports.entry(port).or_default()
    }

    /// Allow connections to an IP subnet.
    ///
    /// Adding the first subnet switches the host from allowing every IP address to allowing only addresses contained
    /// in at least one configured subnet. Denied subnets still take precedence.
    ///
    /// ```
    /// # use datafusion_udf_wasm_host::{AllowCertainHttpRequests, IpNet};
    /// let mut requests = AllowCertainHttpRequests::new();
    /// requests
    ///     .allow_host("api.example.com")
    ///     .allow_subnet("203.0.113.0/24".parse::<IpNet>().unwrap());
    /// ```
    pub fn allow_subnet(&mut self, subnet: IpNet) {
        self.allow_subnets.insert(subnet);
    }

    /// Deny connections to an IP subnet.
    ///
    /// Denied subnets take precedence over allowed subnets, including when the allow-list is empty.
    pub fn deny_subnet(&mut self, subnet: IpNet) {
        self.deny_subnets.insert(subnet);
    }

    /// Check whether an IP address is allowed for this host.
    fn allows_ip(&self, ip: IpAddr) -> bool {
        (self.allow_subnets.is_empty()
            || self.allow_subnets.iter().any(|subnet| subnet.contains(&ip)))
            && !self.deny_subnets.iter().any(|subnet| subnet.contains(&ip))
    }
}

impl ConfigField for AllowHttpHost {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        _description: &'static str,
    ) {
        let Self {
            ports,
            allow_subnets,
            deny_subnets,
        } = self;

        if !allow_subnets.is_empty() {
            v.some(
                &format!("{key}.allow_subnets"),
                allow_subnets
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(Self::SUBNET_SEP),
                "Allowed IP subnet",
            );
        }
        if !deny_subnets.is_empty() {
            v.some(
                &format!("{key}.deny_subnets"),
                deny_subnets
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(Self::SUBNET_SEP),
                "Denied IP subnet",
            );
        }

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
            "allow_subnets" if key.is_empty() => {
                self.allow_subnets = parse_subnets(value).context("parse allowed IP subnets")?;
                Ok(())
            }
            "deny_subnets" if key.is_empty() => {
                self.deny_subnets = parse_subnets(value).context("parse denied IP subnets")?;
                Ok(())
            }
            "allow_subnets" | "deny_subnets" => Err(DataFusionError::Configuration(format!(
                "unknown field: `{field}.{key}`"
            ))),
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

/// Parse a delimited IP subnet list from a DataFusion configuration value.
fn parse_subnets(value: &str) -> DataFusionResult<BTreeSet<IpNet>> {
    value
        .split(AllowHttpHost::SUBNET_SEP)
        .map(|value| {
            value.parse::<IpNet>().map_err(|e| {
                DataFusionError::External(Box::new(e)).context("cannot parse IP subnet")
            })
        })
        .collect()
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
        let hostname = request.uri().host().ok_or(HttpRequestRejected)?;
        let host = self.hosts.get(hostname).ok_or(HttpRequestRejected)?;

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

        if let Ok(ip) = hostname.parse()
            && !host.allows_ip(ip)
        {
            return Err(HttpRequestRejected);
        }

        Ok(())
    }

    fn validate_ip(&self, hostname: &str, ip: IpAddr) -> Result<(), HttpRequestRejected> {
        self.hosts
            .get(hostname)
            .ok_or(HttpRequestRejected)
            .and_then(|host| host.allows_ip(ip).then_some(()).ok_or(HttpRequestRejected))
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
    fn test_subnet_allow_deny() {
        let mut host = AllowHttpHost::default();

        assert!(host.allows_ip("127.0.0.1".parse().unwrap()));
        host.allow_subnet("192.168.0.0/16".parse().unwrap());
        assert!(!host.allows_ip("127.0.0.1".parse().unwrap()));
        assert!(host.allows_ip("192.168.1.1".parse().unwrap()));
        host.deny_subnet("192.168.1.0/24".parse().unwrap());
        assert!(!host.allows_ip("192.168.1.1".parse().unwrap()));
        assert!(host.allows_ip("192.168.2.1".parse().unwrap()));
        host.allow_subnet("2001:db8::/32".parse().unwrap());
        assert!(host.allows_ip("2001:db8::1".parse().unwrap()));
        assert!(!host.allows_ip("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn test_ip_literal_is_checked_against_subnets() {
        let mut policy = AllowCertainHttpRequests::new();
        let host = policy.allow_host("127.0.0.1");
        let endpoint = host.allow_port(HttpConnectionMode::PlainText.default_port());
        endpoint.allow_mode(HttpConnectionMode::PlainText);
        endpoint.allow_method(HttpMethod::GET);
        host.deny_subnet("127.0.0.0/8".parse().unwrap());

        let request = hyper::Request::builder()
            .method(HttpMethod::GET)
            .uri("http://127.0.0.1")
            .body(Default::default())
            .unwrap();

        assert_eq!(
            policy.validate(&request, HttpConnectionMode::PlainText),
            Err(HttpRequestRejected)
        );
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
        host_1.allow_subnet("10.0.0.0/8".parse().unwrap());
        host_1.allow_subnet("2001:db8::/32".parse().unwrap());
        host_1.deny_subnet("10.0.1.0/24".parse().unwrap());
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
        # Allowed IP subnet
        test.host.[foo.bar].allow_subnets=10.0.0.0/8|2001:db8::/32

        # Denied IP subnet
        test.host.[foo.bar].deny_subnets=10.0.1.0/24

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
        let err = config_parsing_err("test.host.[foo].allow_subnets=not-a-subnet");
        assert!(err.to_string().contains("parse allowed IP subnets"));
        assert!(err.to_string().contains("cannot parse IP subnet"));
        for field in ["allow_subnets", "deny_subnets"] {
            let err = config_parsing_err(&format!("test.host.[foo].{field}.typo=127.0.0.0/8"));
            assert!(
                err.to_string()
                    .contains(&format!("unknown field: `{field}.typo`"))
            );
        }
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
