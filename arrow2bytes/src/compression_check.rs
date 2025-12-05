//! Scan IPC data for compressed data.
//!
//! This is a workaround until <https://github.com/apache/arrow-rs/issues/8917> is implemented.

use std::io::{Cursor, Read, Seek};

use arrow::{
    error::ArrowError,
    ipc::{BodyCompression, MessageHeader, root_as_message},
};

/// Detect and fail if there's compressed data.
pub(crate) fn detect_compressed_data(bytes: &[u8]) -> Result<(), ArrowError> {
    let mut reader = Cursor::new(bytes);

    loop {
        let Some(meta_len) = read_meta_len(&mut reader)? else {
            break;
        };
        let mut meta = vec![0; meta_len];
        reader.read_exact(&mut meta)?;
        let msg = root_as_message(&meta).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;

        match msg.header_type() {
            MessageHeader::Schema => {
                // never compressed
            }
            MessageHeader::DictionaryBatch => {
                if let Some(batch) = msg.header_as_dictionary_batch()
                    && let Some(batch) = batch.data()
                    && let Some(compression) = batch.compression()
                {
                    return Err(compression_err("dictionary batch", compression));
                }
            }
            MessageHeader::RecordBatch => {
                if let Some(batch) = msg.header_as_record_batch()
                    && let Some(compression) = batch.compression()
                {
                    return Err(compression_err("record batch", compression));
                }
            }
            x => {
                return Err(ArrowError::ParseError(format!(
                    "Unsupported message header type in IPC stream: '{x:?}'"
                )));
            }
        }

        let body_len = msg.bodyLength();
        if body_len < 0 {
            return Err(ArrowError::ParseError(format!(
                "Invalid body length: {body_len}"
            )));
        }
        reader.seek_relative(body_len)?;
    }

    Ok(())
}

/// Read the metadata length for the next message from the underlying stream.
///
/// # Returns
/// - `Ok(None)` if the reader signals the end of stream with EOF on
///   the first read
/// - `Err(_)` if the reader returns an error other than EOF on the first
///   read, or if the metadata length is less than 0.
/// - `Ok(Some(_))` with the length otherwise.
fn read_meta_len(reader: &mut Cursor<&[u8]>) -> Result<Option<usize>, ArrowError> {
    const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
    let mut meta_len: [u8; 4] = [0; 4];
    match reader.read_exact(&mut meta_len) {
        Ok(_) => {}
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(None)
            } else {
                Err(ArrowError::from(e))
            };
        }
    }

    let meta_len = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_len == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_len)?;
        }

        i32::from_le_bytes(meta_len)
    };

    if meta_len == 0 {
        return Ok(None);
    }

    let meta_len = usize::try_from(meta_len)
        .map_err(|_| ArrowError::ParseError(format!("Invalid metadata length: {meta_len}")))?;

    Ok(Some(meta_len))
}

/// Generate error for encountered compression.
fn compression_err(what: &'static str, compression: BodyCompression<'_>) -> ArrowError {
    ArrowError::IpcError(format!(
        "IPC {what} is compressed using {}, but compressed data MUST NOT cross the security boundary. If you want to handle compressed data, please decompress it within the guest.",
        compression.codec().variant_name().unwrap_or("<unknown>")
    ))
}
