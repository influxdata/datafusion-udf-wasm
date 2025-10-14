//! Python modules that are injected by us.
use pyo3::{BoundObject, exceptions::PyValueError, prelude::*};

mod error;

use error::{ResourceMoved, ResourceMovedOptionExt};

/// Register python modules.
///
/// # Panic
/// This must be called BEFORE the interpreter is used.
pub(crate) fn register() {
    pyo3::append_to_inittab!(wit_world);
}

#[allow(
    clippy::allow_attributes,
    clippy::missing_docs_in_private_items,
    reason = "We're just replicating the existing componentize-py API here."
)]
#[pyo3::pymodule]
mod wit_world {
    use self::types::{ErrWrapperResultExt, ResultWrapper};
    use super::*;

    #[pyo3::pymodule]
    mod imports {
        use self::{
            error::Error,
            poll::Pollable,
            streams::InputStream,
            types::{ErrorCode, FutureIncomingResponse, OutgoingRequest, RequestOptions},
        };
        use super::*;

        /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            Python::attach(|py| {
                py.import("sys")?
                    .getattr("modules")?
                    .set_item("wit_world.imports", m)
            })
        }

        #[pyo3::pymodule]
        mod error {
            use super::*;

            /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
            #[pymodule_init]
            fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
                Python::attach(|py| {
                    py.import("sys")?
                        .getattr("modules")?
                        .set_item("wit_world.imports.error", m)
                })
            }

            #[pyclass]
            #[derive(Debug)]
            pub(crate) struct Error {
                pub(crate) inner: wasip2::io::error::Error,
            }

            #[pymethods]
            impl Error {
                fn to_debug_string(&self) -> String {
                    self.inner.to_debug_string()
                }
            }
        }

        #[pyo3::pymodule]
        mod outgoing_handler {
            use super::*;

            /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
            #[pymodule_init]
            fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
                Python::attach(|py| {
                    py.import("sys")?
                        .getattr("modules")?
                        .set_item("wit_world.imports.outgoing_handler", m)
                })
            }

            #[pyfunction]
            fn handle(
                request: &'_ mut OutgoingRequest,
                options: Option<&'_ mut RequestOptions>,
            ) -> PyResult<FutureIncomingResponse> {
                let request = request.inner.take().require_resource()?;

                let options = options
                    .map(|options| options.inner.take().require_resource())
                    .transpose()?;

                let resp = wasip2::http::outgoing_handler::handle(request, options)
                    .map_err(ErrorCode::from)
                    .to_pyres()?;
                Ok(FutureIncomingResponse { inner: Some(resp) })
            }
        }

        #[pyo3::pymodule]
        mod poll {
            use super::*;

            /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
            #[pymodule_init]
            fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
                Python::attach(|py| {
                    py.import("sys")?
                        .getattr("modules")?
                        .set_item("wit_world.imports.poll", m)
                })
            }

            #[pyclass]
            #[derive(Debug)]
            pub(crate) struct Pollable {
                pub(crate) inner: Option<wasip2::http::types::Pollable>,
            }

            impl Pollable {
                fn inner(&self) -> Result<&wasip2::http::types::Pollable, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl Pollable {
                fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __exit__(
                    &mut self,
                    _exc_type: &Bound<'_, PyAny>,
                    _exc_value: &Bound<'_, PyAny>,
                    _traceback: &Bound<'_, PyAny>,
                ) {
                    self.inner.take();
                }

                fn block(&self) -> PyResult<()> {
                    self.inner()?.block();
                    Ok(())
                }
            }
        }

        #[pyo3::pymodule]
        mod streams {
            use super::*;

            /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
            #[pymodule_init]
            fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
                Python::attach(|py| {
                    py.import("sys")?
                        .getattr("modules")?
                        .set_item("wit_world.imports.streams", m)
                })
            }

            #[pyclass]
            pub(crate) struct InputStream {
                pub(crate) inner: Option<wasip2::http::types::InputStream>,
            }

            impl InputStream {
                fn inner(&self) -> Result<&wasip2::http::types::InputStream, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl InputStream {
                fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __exit__(
                    &mut self,
                    _exc_type: &Bound<'_, PyAny>,
                    _exc_value: &Bound<'_, PyAny>,
                    _traceback: &Bound<'_, PyAny>,
                ) {
                    self.inner.take();
                }

                fn blocking_read(&self, len: u64) -> PyResult<Vec<u8>> {
                    match self.inner()?.blocking_read(len) {
                        Ok(data) => Ok(data),
                        Err(e) => {
                            let e = Python::attach(|py| StreamError::new(py, e))?;
                            Err(e).to_pyres()
                        }
                    }
                }
            }

            #[pyclass]
            #[derive(Debug)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct StreamError_LastOperationFailed {
                value: Py<Error>,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct StreamError_Closed;

            #[derive(Debug, IntoPyObject)]
            pub(crate) enum StreamError {
                #[pyo3(transparent)]
                LastOperationFailed(StreamError_LastOperationFailed),
                #[pyo3(transparent)]
                Closed(StreamError_Closed),
            }

            impl StreamError {
                fn new<'py>(
                    py: Python<'py>,
                    e: wasip2::io::streams::StreamError,
                ) -> PyResult<Self> {
                    use wasip2::io::streams::StreamError as E;

                    let e = match e {
                        E::LastOperationFailed(e) => {
                            let e = Py::new(py, Error { inner: e })?;
                            Self::LastOperationFailed(StreamError_LastOperationFailed { value: e })
                        }
                        E::Closed => Self::Closed(Default::default()),
                    };
                    Ok(e)
                }
            }
        }

        #[pyo3::pymodule]
        mod types {
            use super::*;

            /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
            #[pymodule_init]
            fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
                Python::attach(|py| {
                    py.import("sys")?
                        .getattr("modules")?
                        .set_item("wit_world.imports.types", m)
                })
            }

            #[pyclass]
            #[pyo3(frozen, get_all)]
            #[derive(Debug, Clone)]
            struct DnsErrorPayload {
                rcode: Option<String>,
                info_code: Option<u16>,
            }

            impl From<wasip2::http::types::DnsErrorPayload> for DnsErrorPayload {
                fn from(p: wasip2::http::types::DnsErrorPayload) -> Self {
                    Self {
                        rcode: p.rcode,
                        info_code: p.info_code,
                    }
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DnsTimeout;

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DnsError {
                value: DnsErrorPayload,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DestinationNotFound;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DestinationUnavailable;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DestinationIpProhibited;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_DestinationIpUnroutable;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionRefused;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionTerminated;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionTimeout;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionReadTimeout;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionWriteTimeout;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConnectionLimitReached;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_TlsProtocolError;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_TlsCertificateError;

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_TlsAlertReceived {
                value: TlsAlertReceivedPayload,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestDenied;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestLengthRequired;

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestBodySize {
                value: Option<u64>,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestMethodInvalid;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestUriInvalid;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestUriTooLong;

            #[pyclass]
            #[derive(Debug, Clone, Copy)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestHeaderSectionSize {
                value: Option<u32>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestHeaderSize {
                value: Option<FieldSizePayload>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestTrailerSectionSize {
                value: Option<u32>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpRequestTrailerSize {
                value: FieldSizePayload,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseIncomplete;

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseHeaderSectionSize {
                value: Option<u32>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseHeaderSize {
                value: FieldSizePayload,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseBodySize {
                value: Option<u64>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseTrailerSectionSize {
                value: Option<u32>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseTrailerSize {
                value: FieldSizePayload,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseTransferCoding {
                value: Option<String>,
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseContentCoding {
                value: Option<String>,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpResponseTimeout;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpUpgradeFailed;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_HttpProtocolError;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_LoopDetected;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_ConfigurationError;

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            #[expect(non_camel_case_types)]
            pub(crate) struct ErrorCode_InternalError {
                value: Option<String>,
            }

            #[derive(Debug, IntoPyObject)]
            pub(crate) enum ErrorCode {
                #[pyo3(transparent)]
                DnsTimeout(ErrorCode_DnsTimeout),
                #[pyo3(transparent)]
                DnsError(ErrorCode_DnsError),
                #[pyo3(transparent)]
                DestinationNotFound(ErrorCode_DestinationNotFound),
                #[pyo3(transparent)]
                DestinationUnavailable(ErrorCode_DestinationUnavailable),
                #[pyo3(transparent)]
                DestinationIpProhibited(ErrorCode_DestinationIpProhibited),
                #[pyo3(transparent)]
                DestinationIpUnroutable(ErrorCode_DestinationIpUnroutable),
                #[pyo3(transparent)]
                ConnectionRefused(ErrorCode_ConnectionRefused),
                #[pyo3(transparent)]
                ConnectionTerminated(ErrorCode_ConnectionTerminated),
                #[pyo3(transparent)]
                ConnectionTimeout(ErrorCode_ConnectionTimeout),
                #[pyo3(transparent)]
                ConnectionReadTimeout(ErrorCode_ConnectionReadTimeout),
                #[pyo3(transparent)]
                ConnectionWriteTimeout(ErrorCode_ConnectionWriteTimeout),
                #[pyo3(transparent)]
                ConnectionLimitReached(ErrorCode_ConnectionLimitReached),
                #[pyo3(transparent)]
                TlsProtocolError(ErrorCode_TlsProtocolError),
                #[pyo3(transparent)]
                TlsCertificateError(ErrorCode_TlsCertificateError),
                #[pyo3(transparent)]
                TlsAlertReceived(ErrorCode_TlsAlertReceived),
                #[pyo3(transparent)]
                HttpRequestDenied(ErrorCode_HttpRequestDenied),
                #[pyo3(transparent)]
                HttpRequestLengthRequired(ErrorCode_HttpRequestLengthRequired),
                #[pyo3(transparent)]
                HttpRequestBodySize(ErrorCode_HttpRequestBodySize),
                #[pyo3(transparent)]
                HttpRequestMethodInvalid(ErrorCode_HttpRequestMethodInvalid),
                #[pyo3(transparent)]
                HttpRequestUriInvalid(ErrorCode_HttpRequestUriInvalid),
                #[pyo3(transparent)]
                HttpRequestUriTooLong(ErrorCode_HttpRequestUriTooLong),
                #[pyo3(transparent)]
                HttpRequestHeaderSectionSize(ErrorCode_HttpRequestHeaderSectionSize),
                #[pyo3(transparent)]
                HttpRequestHeaderSize(ErrorCode_HttpRequestHeaderSize),
                #[pyo3(transparent)]
                HttpRequestTrailerSectionSize(ErrorCode_HttpRequestTrailerSectionSize),
                #[pyo3(transparent)]
                HttpRequestTrailerSize(ErrorCode_HttpRequestTrailerSize),
                #[pyo3(transparent)]
                HttpResponseIncomplete(ErrorCode_HttpResponseIncomplete),
                #[pyo3(transparent)]
                HttpResponseHeaderSectionSize(ErrorCode_HttpResponseHeaderSectionSize),
                #[pyo3(transparent)]
                HttpResponseHeaderSize(ErrorCode_HttpResponseHeaderSize),
                #[pyo3(transparent)]
                HttpResponseBodySize(ErrorCode_HttpResponseBodySize),
                #[pyo3(transparent)]
                HttpResponseTrailerSectionSize(ErrorCode_HttpResponseTrailerSectionSize),
                #[pyo3(transparent)]
                HttpResponseTrailerSize(ErrorCode_HttpResponseTrailerSize),
                #[pyo3(transparent)]
                HttpResponseTransferCoding(ErrorCode_HttpResponseTransferCoding),
                #[pyo3(transparent)]
                HttpResponseContentCoding(ErrorCode_HttpResponseContentCoding),
                #[pyo3(transparent)]
                HttpResponseTimeout(ErrorCode_HttpResponseTimeout),
                #[pyo3(transparent)]
                HttpUpgradeFailed(ErrorCode_HttpUpgradeFailed),
                #[pyo3(transparent)]
                HttpProtocolError(ErrorCode_HttpProtocolError),
                #[pyo3(transparent)]
                LoopDetected(ErrorCode_LoopDetected),
                #[pyo3(transparent)]
                ConfigurationError(ErrorCode_ConfigurationError),
                #[pyo3(transparent)]
                InternalError(ErrorCode_InternalError),
            }

            impl From<wasip2::http::types::ErrorCode> for ErrorCode {
                fn from(e: wasip2::http::types::ErrorCode) -> Self {
                    use wasip2::http::types::ErrorCode as E;
                    match e {
                        E::DnsTimeout => Self::DnsTimeout(Default::default()),
                        E::DnsError(dns_error_payload) => Self::DnsError(ErrorCode_DnsError {
                            value: dns_error_payload.into(),
                        }),
                        E::DestinationNotFound => Self::DestinationNotFound(Default::default()),
                        E::DestinationUnavailable => {
                            Self::DestinationUnavailable(Default::default())
                        }
                        E::DestinationIpProhibited => {
                            Self::DestinationIpProhibited(Default::default())
                        }
                        E::DestinationIpUnroutable => {
                            Self::DestinationIpUnroutable(Default::default())
                        }
                        E::ConnectionRefused => Self::ConnectionRefused(Default::default()),
                        E::ConnectionTerminated => Self::ConnectionTerminated(Default::default()),
                        E::ConnectionTimeout => Self::ConnectionTimeout(Default::default()),
                        E::ConnectionReadTimeout => Self::ConnectionReadTimeout(Default::default()),
                        E::ConnectionWriteTimeout => {
                            Self::ConnectionWriteTimeout(Default::default())
                        }
                        E::ConnectionLimitReached => {
                            Self::ConnectionLimitReached(Default::default())
                        }
                        E::TlsProtocolError => Self::TlsProtocolError(Default::default()),
                        E::TlsCertificateError => Self::TlsCertificateError(Default::default()),
                        E::TlsAlertReceived(tls_alert_received_payload) => {
                            Self::TlsAlertReceived(ErrorCode_TlsAlertReceived {
                                value: tls_alert_received_payload.into(),
                            })
                        }
                        E::HttpRequestDenied => Self::HttpRequestDenied(Default::default()),
                        E::HttpRequestLengthRequired => {
                            Self::HttpRequestLengthRequired(Default::default())
                        }
                        E::HttpRequestBodySize(size) => {
                            Self::HttpRequestBodySize(ErrorCode_HttpRequestBodySize { value: size })
                        }
                        E::HttpRequestMethodInvalid => {
                            Self::HttpRequestMethodInvalid(Default::default())
                        }
                        E::HttpRequestUriInvalid => Self::HttpRequestUriInvalid(Default::default()),
                        E::HttpRequestUriTooLong => Self::HttpRequestUriTooLong(Default::default()),
                        E::HttpRequestHeaderSectionSize(size) => {
                            Self::HttpRequestHeaderSectionSize(
                                ErrorCode_HttpRequestHeaderSectionSize { value: size },
                            )
                        }
                        E::HttpRequestHeaderSize(field_size_payload) => {
                            Self::HttpRequestHeaderSize(ErrorCode_HttpRequestHeaderSize {
                                value: field_size_payload.map(|x| x.into()),
                            })
                        }
                        E::HttpRequestTrailerSectionSize(size) => {
                            Self::HttpRequestTrailerSectionSize(
                                ErrorCode_HttpRequestTrailerSectionSize { value: size },
                            )
                        }
                        E::HttpRequestTrailerSize(field_size_payload) => {
                            Self::HttpRequestTrailerSize(ErrorCode_HttpRequestTrailerSize {
                                value: field_size_payload.into(),
                            })
                        }
                        E::HttpResponseIncomplete => {
                            Self::HttpResponseIncomplete(Default::default())
                        }
                        E::HttpResponseHeaderSectionSize(size) => {
                            Self::HttpResponseHeaderSectionSize(
                                ErrorCode_HttpResponseHeaderSectionSize { value: size },
                            )
                        }
                        E::HttpResponseHeaderSize(field_size_payload) => {
                            Self::HttpResponseHeaderSize(ErrorCode_HttpResponseHeaderSize {
                                value: field_size_payload.into(),
                            })
                        }
                        E::HttpResponseBodySize(size) => {
                            Self::HttpResponseBodySize(ErrorCode_HttpResponseBodySize {
                                value: size,
                            })
                        }
                        E::HttpResponseTrailerSectionSize(size) => {
                            Self::HttpResponseTrailerSectionSize(
                                ErrorCode_HttpResponseTrailerSectionSize { value: size },
                            )
                        }
                        E::HttpResponseTrailerSize(field_size_payload) => {
                            Self::HttpResponseTrailerSize(ErrorCode_HttpResponseTrailerSize {
                                value: field_size_payload.into(),
                            })
                        }
                        E::HttpResponseTransferCoding(coding) => {
                            Self::HttpResponseTransferCoding(ErrorCode_HttpResponseTransferCoding {
                                value: coding,
                            })
                        }
                        E::HttpResponseContentCoding(coding) => {
                            Self::HttpResponseContentCoding(ErrorCode_HttpResponseContentCoding {
                                value: coding,
                            })
                        }
                        E::HttpResponseTimeout => Self::HttpResponseTimeout(Default::default()),
                        E::HttpUpgradeFailed => Self::HttpUpgradeFailed(Default::default()),
                        E::HttpProtocolError => Self::HttpProtocolError(Default::default()),
                        E::LoopDetected => Self::LoopDetected(Default::default()),
                        E::ConfigurationError => Self::ConfigurationError(Default::default()),
                        E::InternalError(msg) => {
                            Self::InternalError(ErrorCode_InternalError { value: msg })
                        }
                    }
                }
            }

            #[pyclass]
            struct Fields {
                inner: Option<wasip2::http::types::Fields>,
            }

            impl Fields {
                fn inner(&self) -> Result<&wasip2::http::types::Fields, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl Fields {
                #[new]
                fn new() -> Self {
                    Self {
                        inner: Some(wasip2::http::types::Fields::new()),
                    }
                }

                fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __exit__(
                    &mut self,
                    _exc_type: &Bound<'_, PyAny>,
                    _exc_value: &Bound<'_, PyAny>,
                    _traceback: &Bound<'_, PyAny>,
                ) {
                    self.inner.take();
                }

                fn append(&self, name: String, value: Vec<u8>) -> PyResult<()> {
                    self.inner()?
                        .append(&name, &value)
                        .map_err(HeaderError::from)
                        .to_pyres()?;
                    Ok(())
                }

                fn entries(&self) -> PyResult<Vec<(String, Vec<u8>)>> {
                    let entries = self.inner()?.entries();
                    Ok(entries)
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone)]
            #[pyo3(frozen, get_all)]
            struct FieldSizePayload {
                field_name: Option<String>,
                field_size: Option<u32>,
            }

            impl From<wasip2::http::types::FieldSizePayload> for FieldSizePayload {
                fn from(p: wasip2::http::types::FieldSizePayload) -> Self {
                    Self {
                        field_name: p.field_name,
                        field_size: p.field_size,
                    }
                }
            }

            #[pyclass]
            pub(crate) struct FutureIncomingResponse {
                pub(crate) inner: Option<wasip2::http::types::FutureIncomingResponse>,
            }

            impl FutureIncomingResponse {
                fn inner(
                    &self,
                ) -> Result<&wasip2::http::types::FutureIncomingResponse, ResourceMoved>
                {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl FutureIncomingResponse {
                fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __exit__(
                    &mut self,
                    _exc_type: &Bound<'_, PyAny>,
                    _exc_value: &Bound<'_, PyAny>,
                    _traceback: &Bound<'_, PyAny>,
                ) {
                    self.inner.take();
                }

                fn subscribe(&self) -> PyResult<Pollable> {
                    Ok(Pollable {
                        inner: Some(self.inner()?.subscribe()),
                    })
                }

                fn get(&self) -> PyResult<Option<ResultWrapper>> {
                    let inner = self.inner()?;

                    // this is the official return signature will no type erased
                    let res: Option<Result<Result<IncomingResponse, ErrorCode>, ()>> =
                        inner.get().map(|res| {
                            res.map(|res| {
                                res.map(|resp| IncomingResponse { inner: resp })
                                    .map_err(|code| code.into())
                            })
                        });
                    let Some(res) = res else { return Ok(None) };
                    let res = Python::attach(|py| {
                        let res = res.map(|res| ResultWrapper::new(res, py).unwrap());
                        ResultWrapper::new(res, py).unwrap()
                    });

                    Ok(Some(res))
                }
            }

            #[pyclass]
            struct FutureTrailers {
                #[expect(dead_code)]
                inner: wasip2::http::types::FutureTrailers,
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct HeaderError_InvalidSyntax;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct HeaderError_Forbidden;

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct HeaderError_Immutable;

            #[derive(Debug, IntoPyObject)]
            enum HeaderError {
                #[pyo3(transparent)]
                InvalidSyntax(HeaderError_InvalidSyntax),
                #[pyo3(transparent)]
                Forbidden(HeaderError_Forbidden),
                #[pyo3(transparent)]
                Immutable(HeaderError_Immutable),
            }

            impl From<wasip2::http::types::HeaderError> for HeaderError {
                fn from(e: wasip2::http::types::HeaderError) -> Self {
                    match e {
                        wasip2::http::types::HeaderError::InvalidSyntax => {
                            Self::InvalidSyntax(Default::default())
                        }
                        wasip2::http::types::HeaderError::Forbidden => {
                            Self::Forbidden(Default::default())
                        }
                        wasip2::http::types::HeaderError::Immutable => {
                            Self::Immutable(Default::default())
                        }
                    }
                }
            }

            #[pyclass]
            #[derive(Debug)]
            struct IncomingBody {
                inner: Option<wasip2::http::types::IncomingBody>,
            }

            impl IncomingBody {
                fn inner(&self) -> Result<&wasip2::http::types::IncomingBody, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl IncomingBody {
                fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __exit__(
                    &mut self,
                    _exc_type: &Bound<'_, PyAny>,
                    _exc_value: &Bound<'_, PyAny>,
                    _traceback: &Bound<'_, PyAny>,
                ) {
                    self.inner.take();
                }

                fn stream(&self) -> PyResult<InputStream> {
                    let stream = self.inner()?.stream().to_pyres()?;
                    Ok(InputStream {
                        inner: Some(stream),
                    })
                }
            }

            #[pyclass]
            #[derive(Debug)]
            struct IncomingResponse {
                inner: wasip2::http::types::IncomingResponse,
            }

            #[pymethods]
            impl IncomingResponse {
                fn consume(&self) -> PyResult<IncomingBody> {
                    let body = self.inner.consume().to_pyres()?;
                    Ok(IncomingBody { inner: Some(body) })
                }

                fn headers(&self) -> Fields {
                    Fields {
                        inner: Some(self.inner.headers()),
                    }
                }

                fn status(&self) -> u16 {
                    self.inner.status()
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Get;

            #[pymethods]
            impl Method_Get {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Head;

            #[pymethods]
            impl Method_Head {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Post;

            #[pymethods]
            impl Method_Post {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Put;

            #[pymethods]
            impl Method_Put {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Delete;

            #[pymethods]
            impl Method_Delete {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Connect;

            #[pymethods]
            impl Method_Connect {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Options;

            #[pymethods]
            impl Method_Options {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Trace;

            #[pymethods]
            impl Method_Trace {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone, Copy)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Patch;

            #[pymethods]
            impl Method_Patch {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Default, Clone)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Method_Other {
                value: String,
            }

            #[pymethods]
            impl Method_Other {
                #[new]
                fn new(value: String) -> Self {
                    Self { value }
                }
            }

            #[derive(Debug, IntoPyObject, FromPyObject)]
            enum Method {
                #[pyo3(transparent)]
                Get(Method_Get),
                #[pyo3(transparent)]
                Head(Method_Head),
                #[pyo3(transparent)]
                Post(Method_Post),
                #[pyo3(transparent)]
                Put(Method_Put),
                #[pyo3(transparent)]
                Delete(Method_Delete),
                #[pyo3(transparent)]
                Connect(Method_Connect),
                #[pyo3(transparent)]
                Options(Method_Options),
                #[pyo3(transparent)]
                Trace(Method_Trace),
                #[pyo3(transparent)]
                Patch(Method_Patch),
                #[pyo3(transparent)]
                Other(Method_Other),
            }

            impl From<Method> for wasip2::http::types::Method {
                fn from(method: Method) -> Self {
                    match method {
                        Method::Get(_) => Self::Get,
                        Method::Head(_) => Self::Head,
                        Method::Post(_) => Self::Post,
                        Method::Put(_) => Self::Put,
                        Method::Delete(_) => Self::Delete,
                        Method::Connect(_) => Self::Connect,
                        Method::Options(_) => Self::Options,
                        Method::Trace(_) => Self::Trace,
                        Method::Patch(_) => Self::Patch,
                        Method::Other(method_other) => Self::Other(method_other.value),
                    }
                }
            }

            #[pyclass]
            struct OutgoingBody {
                inner: Option<wasip2::http::types::OutgoingBody>,
            }

            #[pymethods]
            impl OutgoingBody {
                fn finish(&mut self, trailers: Option<&'_ mut Fields>) -> PyResult<()> {
                    let body = self.inner.take().require_resource()?;
                    let trailers = trailers
                        .map(|trailers| trailers.inner.take().require_resource())
                        .transpose()?;
                    wasip2::http::types::OutgoingBody::finish(body, trailers)
                        .map_err(ErrorCode::from)
                        .to_pyres()?;
                    Ok(())
                }
            }

            #[pyclass]
            pub(crate) struct OutgoingRequest {
                pub(crate) inner: Option<wasip2::http::types::OutgoingRequest>,
            }

            impl OutgoingRequest {
                fn inner(&self) -> Result<&wasip2::http::types::OutgoingRequest, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl OutgoingRequest {
                #[new]
                fn new(headers: &'_ mut Fields) -> PyResult<Self> {
                    let headers = headers.inner.take().require_resource()?;
                    Ok(Self {
                        inner: Some(wasip2::http::types::OutgoingRequest::new(headers)),
                    })
                }

                fn body(&self) -> PyResult<OutgoingBody> {
                    let body = self.inner()?.body().to_pyres()?;
                    Ok(OutgoingBody { inner: Some(body) })
                }

                fn set_authority(&self, authority: Option<String>) -> PyResult<()> {
                    self.inner()?
                        .set_authority(authority.as_deref())
                        .to_pyres()?;
                    Ok(())
                }

                fn set_method(&self, method: Method) -> PyResult<()> {
                    self.inner()?.set_method(&method.into()).to_pyres()?;
                    Ok(())
                }

                fn set_path_with_query(&self, path_with_query: Option<String>) -> PyResult<()> {
                    self.inner()?
                        .set_path_with_query(path_with_query.as_deref())
                        .to_pyres()?;
                    Ok(())
                }

                fn set_scheme(&self, scheme: Option<Scheme>) -> PyResult<()> {
                    self.inner()?
                        .set_scheme(scheme.map(|s| s.into()).as_ref())
                        .to_pyres()?;
                    Ok(())
                }
            }

            #[pyclass]
            pub(crate) struct RequestOptions {
                pub(crate) inner: Option<wasip2::http::types::RequestOptions>,
            }

            impl RequestOptions {
                fn inner(&self) -> Result<&wasip2::http::types::RequestOptions, ResourceMoved> {
                    self.inner.as_ref().require_resource()
                }
            }

            #[pymethods]
            impl RequestOptions {
                #[new]
                fn new() -> Self {
                    Self {
                        inner: Some(wasip2::http::types::RequestOptions::new()),
                    }
                }

                fn set_connect_timeout(&self, duration: Option<u64>) -> PyResult<()> {
                    self.inner()?.set_connect_timeout(duration).to_pyres()?;
                    Ok(())
                }
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Scheme_Http;

            #[pymethods]
            impl Scheme_Http {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Scheme_Https;

            #[pymethods]
            impl Scheme_Https {
                #[new]
                fn new() -> Self {
                    Self
                }
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen)]
            #[expect(non_camel_case_types)]
            struct Scheme_Other {
                value: String,
            }

            #[pymethods]
            impl Scheme_Other {
                #[new]
                fn new(value: String) -> Self {
                    Self { value }
                }
            }

            #[derive(Debug, IntoPyObject, FromPyObject)]
            enum Scheme {
                #[pyo3(transparent)]
                Http(Scheme_Http),
                #[pyo3(transparent)]
                Https(Scheme_Https),
                #[pyo3(transparent)]
                Other(Scheme_Other),
            }

            impl From<Scheme> for wasip2::http::types::Scheme {
                fn from(scheme: Scheme) -> Self {
                    match scheme {
                        Scheme::Http(_) => Self::Http,
                        Scheme::Https(_) => Self::Https,
                        Scheme::Other(scheme_other) => Self::Other(scheme_other.value),
                    }
                }
            }

            #[pyclass]
            #[derive(Debug, Clone)]
            #[pyo3(frozen, get_all)]
            struct TlsAlertReceivedPayload {
                alert_id: Option<u8>,
                alert_message: Option<String>,
            }

            impl From<wasip2::http::types::TlsAlertReceivedPayload> for TlsAlertReceivedPayload {
                fn from(p: wasip2::http::types::TlsAlertReceivedPayload) -> Self {
                    Self {
                        alert_id: p.alert_id,
                        alert_message: p.alert_message,
                    }
                }
            }
        }
    }

    #[pyo3::pymodule]
    pub(crate) mod types {
        use super::*;

        /// Hack: workaround for <https://github.com/PyO3/pyo3/issues/759>.
        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            Python::attach(|py| {
                py.import("sys")?
                    .getattr("modules")?
                    .set_item("wit_world.types", m)
            })
        }

        #[pyclass]
        #[derive(Debug)]
        #[pyo3(frozen, get_all, name = "Ok")]
        pub(crate) struct OkWrapper {
            value: Py<PyAny>,
        }

        #[pyclass]
        #[derive(Debug, IntoPyObject)]
        #[pyo3(extends = PyValueError, frozen, get_all, name = "Err")]
        pub(crate) struct ErrWrapper {
            value: Py<PyAny>,
        }

        impl ErrWrapper {
            pub(crate) fn new<'py, E>(e: E, py: Python<'py>) -> PyResult<Self>
            where
                E: IntoPyObject<'py>,
            {
                Ok(Self {
                    value: e
                        .into_pyobject(py)
                        .map_err(|e| {
                            let e: PyErr = e.into();
                            e
                        })?
                        .into_any()
                        .unbind(),
                })
            }
        }

        pub(crate) trait ErrWrapperResultExt {
            type T;

            fn to_pyres(self) -> PyResult<Self::T>;
        }

        impl<T, E> ErrWrapperResultExt for Result<T, E>
        where
            E: for<'py> IntoPyObject<'py>,
        {
            type T = T;

            fn to_pyres(self) -> PyResult<Self::T> {
                match self {
                    Ok(x) => Ok(x),
                    Err(e) => Python::attach(|py| {
                        let e = ErrWrapper::new(e, py)?;
                        let e = Bound::new(py, e)?;
                        Err(e.into_super().into())
                    }),
                }
            }
        }

        #[derive(Debug, IntoPyObject)]
        pub(crate) enum ResultWrapper {
            #[pyo3(transparent)]
            Ok(OkWrapper),
            #[pyo3(transparent)]
            Err(ErrWrapper),
        }

        impl ResultWrapper {
            pub(crate) fn new<'py, T, E>(res: Result<T, E>, py: Python<'py>) -> PyResult<Self>
            where
                T: IntoPyObject<'py>,
                E: IntoPyObject<'py>,
            {
                let res = match res {
                    Ok(val) => Self::Ok(OkWrapper {
                        value: val
                            .into_pyobject(py)
                            .map_err(|e| {
                                let e: PyErr = e.into();
                                e
                            })?
                            .into_any()
                            .unbind(),
                    }),
                    Err(val) => Self::Err(ErrWrapper {
                        value: val
                            .into_pyobject(py)
                            .map_err(|e| {
                                let e: PyErr = e.into();
                                e
                            })?
                            .into_any()
                            .unbind(),
                    }),
                };
                Ok(res)
            }
        }
    }
}
