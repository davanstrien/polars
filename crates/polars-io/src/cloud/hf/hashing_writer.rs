//! Streaming SHA256 hash computation while writing.
//!
//! This module provides a writer wrapper that computes SHA256 hash incrementally
//! as data flows through, without buffering. This is required for HF Hub LFS uploads,
//! which need the file hash before providing presigned upload URLs.

use std::io::{self, Write};

use sha2::{Digest, Sha256};

/// A writer that computes SHA256 hash of all data written through it.
///
/// This is used for HF Hub LFS uploads, which require the file hash
/// before providing upload URLs.
///
/// # Example
///
/// ```ignore
/// use std::io::Write;
/// use polars_io::cloud::hf::HashingWriter;
///
/// let buffer = Vec::new();
/// let mut writer = HashingWriter::new(buffer);
/// writer.write_all(b"hello world").unwrap();
///
/// let (buffer, hash, bytes) = writer.finish();
/// assert_eq!(bytes, 11);
/// ```
pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<W> HashingWriter<W> {
    /// Create a new HashingWriter wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }

    /// Returns the number of bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Consume the writer and return (inner_writer, sha256_bytes, bytes_written).
    ///
    /// The hash is returned as a 32-byte array. Use [`sha256_to_hex`] to convert
    /// to a lowercase hex string if needed.
    pub fn finish(self) -> (W, [u8; 32], u64) {
        let hash_bytes: [u8; 32] = self.hasher.finalize().into();
        (self.inner, hash_bytes, self.bytes_written)
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        if n > 0 {
            self.hasher.update(&buf[..n]);
            self.bytes_written += n as u64;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Format SHA256 hash bytes as lowercase hex string (64 characters).
///
/// This is the format required by HF Hub LFS API.
pub fn sha256_to_hex(hash: &[u8; 32]) -> String {
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_empty_write() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let writer = HashingWriter::new(cursor);
        let (cursor, hash, bytes) = writer.finish();

        // SHA256 of empty string
        assert_eq!(bytes, 0);
        assert_eq!(
            sha256_to_hex(&hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(cursor.into_inner().len(), 0);
    }

    #[test]
    fn test_single_write() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = HashingWriter::new(cursor);
        writer.write_all(b"hello world").unwrap();

        let (cursor, hash, bytes) = writer.finish();

        assert_eq!(bytes, 11);
        assert_eq!(
            sha256_to_hex(&hash),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert_eq!(cursor.into_inner(), b"hello world");
    }

    #[test]
    fn test_multiple_writes() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = HashingWriter::new(cursor);
        writer.write_all(b"hello").unwrap();
        writer.write_all(b" ").unwrap();
        writer.write_all(b"world").unwrap();

        let (cursor, hash, bytes) = writer.finish();

        assert_eq!(bytes, 11);
        // Same hash as single write - hash is of the full content
        assert_eq!(
            sha256_to_hex(&hash),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert_eq!(cursor.into_inner(), b"hello world");
    }

    #[test]
    fn test_bytes_written_tracking() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = HashingWriter::new(cursor);

        assert_eq!(writer.bytes_written(), 0);
        writer.write_all(b"test").unwrap();
        assert_eq!(writer.bytes_written(), 4);
        writer.write_all(b"data").unwrap();
        assert_eq!(writer.bytes_written(), 8);
    }

    #[test]
    fn test_flush_passthrough() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = HashingWriter::new(cursor);
        writer.write_all(b"data").unwrap();
        // Should not panic
        writer.flush().unwrap();
    }

    #[test]
    fn test_sha256_to_hex() {
        let hash: [u8; 32] = [0; 32];
        assert_eq!(
            sha256_to_hex(&hash),
            "0000000000000000000000000000000000000000000000000000000000000000"
        );

        let hash: [u8; 32] = [0xff; 32];
        assert_eq!(
            sha256_to_hex(&hash),
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
    }

    #[test]
    fn test_partial_write() {
        // Test that we correctly handle partial writes
        // Using a writer that only writes one byte at a time
        struct OneByteWriter(Vec<u8>);
        impl Write for OneByteWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                if buf.is_empty() {
                    return Ok(0);
                }
                self.0.push(buf[0]);
                Ok(1)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = HashingWriter::new(OneByteWriter(Vec::new()));
        // write() may only write 1 byte, so we use write_all which retries
        writer.write_all(b"abc").unwrap();

        let (inner, hash, bytes) = writer.finish();
        assert_eq!(bytes, 3);
        assert_eq!(inner.0, b"abc");
        // SHA256 of "abc"
        assert_eq!(
            sha256_to_hex(&hash),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
