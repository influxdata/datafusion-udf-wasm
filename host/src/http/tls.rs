//! TLS-related configs.

use std::sync::Arc;

use reqwest::{Certificate, tls::Version as TlsVersion};

/// TLS client config.
#[derive(Debug, Clone)]
pub struct TlsClientConfig {
    /// CAs.
    pub(crate) ca_certs: Arc<[Certificate]>,

    /// Exclude bundled root CAs.
    pub(crate) exclude_bundled_ca_certs: bool,

    /// Minimum TLS version.
    pub(crate) version_min: TlsVersion,
}

impl TlsClientConfig {
    /// Set CAs.
    ///
    /// Calling this method a second time will override the first list.
    pub fn with_ca_certs(self, certs: impl IntoIterator<Item = Certificate>) -> Self {
        Self {
            ca_certs: certs.into_iter().collect(),
            ..self
        }
    }

    /// Exclude bundled root CAs.
    ///
    /// This will lead to an empty list of root CAs. Use [`with_ca_certs`](Self::with_ca_certs) to add your own.
    ///
    /// Certificates already added by [`with_ca_certs`](Self::with_ca_certs) will NOT be erased by calling
    /// [`exclude_bundled_ca_certs`](Self::exclude_bundled_ca_certs).
    pub fn exclude_bundled_ca_certs(self, exclude: bool) -> Self {
        Self {
            exclude_bundled_ca_certs: exclude,
            ..self
        }
    }

    /// Minimum TLS version.
    pub fn version_min(self, version: TlsVersion) -> Self {
        Self {
            version_min: version,
            ..self
        }
    }
}

impl Default for TlsClientConfig {
    fn default() -> Self {
        Self {
            ca_certs: Default::default(),
            exclude_bundled_ca_certs: false,
            version_min: TlsVersion::TLS_1_3,
        }
    }
}
