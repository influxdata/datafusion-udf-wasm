//! Common types used for HTTP routines.
use std::{fmt, num::NonZeroU16, str::FromStr};

pub use http::Method as HttpMethod;

/// An HTTP port.
///
/// Can be any [`u16`] value except for zero.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct HttpPort(NonZeroU16);

impl HttpPort {
    /// Create new port from [`u16`].
    ///
    /// Returns [`None`] if port is zero.
    pub const fn new(p: u16) -> Option<Self> {
        // NOTE: `Option::map` isn't const-stable
        match NonZeroU16::new(p) {
            Some(p) => Some(Self(p)),
            None => None,
        }
    }

    /// Get [`u16`] representation of that port.
    pub const fn get_u16(&self) -> u16 {
        self.0.get()
    }
}

impl std::fmt::Debug for HttpPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(f)
    }
}

impl std::fmt::Display for HttpPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(f)
    }
}

impl std::str::FromStr for HttpPort {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let p: NonZeroU16 = s.parse()?;
        Ok(Self(p))
    }
}

/// HTTP connection mode.
///
/// Defaults to [`Encrypted`](Self::Encrypted).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub enum HttpConnectionMode {
    /// Encrypted via TLS, i.e. HTTPs.
    #[default]
    Encrypted,

    /// Unencrypted, i.e. plain HTTP.
    PlainText,
}

impl HttpConnectionMode {
    /// Default port for this connection mode.
    pub const fn default_port(&self) -> HttpPort {
        match self {
            Self::Encrypted => HttpPort::new(443).expect("valid port"),
            Self::PlainText => HttpPort::new(80).expect("valid port"),
        }
    }

    /// Derive mode from boolean "use TLS?" flag.
    pub(crate) fn from_use_tls(use_tls: bool) -> Self {
        if use_tls {
            Self::Encrypted
        } else {
            Self::PlainText
        }
    }

    /// Represent mode as string.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Encrypted => "encrypted",
            Self::PlainText => "plaintext",
        }
    }
}

impl std::fmt::Display for HttpConnectionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Invalid [`HttpConnectionMode`].
#[derive(Debug)]
pub struct InvalidHttpConnectionMode(String);

impl std::fmt::Display for InvalidHttpConnectionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid HTTP connection mode: `{}`", self.0)
    }
}

impl std::error::Error for InvalidHttpConnectionMode {}

impl FromStr for HttpConnectionMode {
    type Err = InvalidHttpConnectionMode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_ascii_lowercase();
        match s_lower.as_str() {
            "encrypted" => Ok(Self::Encrypted),
            "plaintext" => Ok(Self::PlainText),
            _ => Err(InvalidHttpConnectionMode(s.to_owned())),
        }
    }
}
