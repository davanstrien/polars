//! Memory-mapped temp file buffer for HF Hub uploads.
//!
//! This module provides a write buffer backed by a memory-mapped temporary file.
//! Data is written via the `Write` trait, and can be read back via `as_slice()`
//! or by converting to a read-only `MmapReadHandle` for upload.
//!
//! The buffer grows dynamically when capacity is exceeded, doubling in size
//! (with a minimum of 1MB). The temp file is automatically deleted when the
//! buffer or read handle is dropped.

use std::io::{self, Write};

use memmap::{Mmap, MmapMut};
use tempfile::NamedTempFile;

/// Default initial capacity: 1 MiB.
const DEFAULT_INITIAL_CAPACITY: usize = 1024 * 1024;

/// Minimum capacity for growth: 1 MiB.
const MIN_CAPACITY: usize = 1024 * 1024;

/// A write buffer backed by a memory-mapped temporary file.
///
/// Data is written incrementally via the `Write` trait. The buffer grows
/// automatically when capacity is exceeded. Once writing is complete,
/// convert to an `MmapReadHandle` for efficient read-back during upload.
///
/// # Example
///
/// ```ignore
/// use std::io::Write;
/// use polars_io::cloud::hf::MmapBuffer;
///
/// let mut buffer = MmapBuffer::new(1024)?;
/// buffer.write_all(b"hello world")?;
///
/// let handle = buffer.into_read_handle()?;
/// assert_eq!(handle.as_slice(), b"hello world");
/// ```
pub struct MmapBuffer {
    /// The underlying temp file (keeps file alive).
    file: NamedTempFile,
    /// Mutable memory map. None if capacity is 0 (mmap requires len > 0).
    mmap: Option<MmapMut>,
    /// Number of bytes written so far.
    len: usize,
    /// Current mmap/file capacity.
    capacity: usize,
}

impl MmapBuffer {
    /// Create a new MmapBuffer with the given initial capacity.
    ///
    /// If `initial_capacity` is 0, it defaults to 1 MiB.
    pub fn new(initial_capacity: usize) -> io::Result<Self> {
        let capacity = if initial_capacity == 0 {
            DEFAULT_INITIAL_CAPACITY
        } else {
            initial_capacity
        };

        let file = NamedTempFile::new()?;
        file.as_file().set_len(capacity as u64)?;

        // SAFETY: We just created the file and set its length.
        // The file is exclusively owned by us.
        let mmap = unsafe { MmapMut::map_mut(file.as_file())? };

        Ok(Self {
            file,
            mmap: Some(mmap),
            len: 0,
            capacity,
        })
    }

    /// Create a new MmapBuffer with default initial capacity (1 MiB).
    pub fn with_default_capacity() -> io::Result<Self> {
        Self::new(DEFAULT_INITIAL_CAPACITY)
    }

    /// Returns the number of bytes written to the buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if no bytes have been written.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the current capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the written bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        match &self.mmap {
            Some(mmap) => &mmap[..self.len],
            None => &[],
        }
    }

    /// Grow the buffer to at least `min_capacity`.
    ///
    /// The new capacity is the maximum of:
    /// - `min_capacity`
    /// - Current capacity doubled
    /// - `MIN_CAPACITY` (1 MiB)
    fn grow(&mut self, min_capacity: usize) -> io::Result<()> {
        // Calculate new capacity: at least double, at least min_capacity, at least MIN_CAPACITY
        let new_capacity = self
            .capacity
            .saturating_mul(2)
            .max(min_capacity)
            .max(MIN_CAPACITY);

        // Drop existing mmap before resizing file
        self.mmap = None;

        // Resize the file
        self.file.as_file().set_len(new_capacity as u64)?;

        // Create new mmap
        // SAFETY: We own the file exclusively and just resized it.
        let mmap = unsafe { MmapMut::map_mut(self.file.as_file())? };
        self.mmap = Some(mmap);
        self.capacity = new_capacity;

        Ok(())
    }

    /// Finalize the buffer and convert to a read-only handle.
    ///
    /// This flushes any pending writes and converts the mutable mmap
    /// to a read-only mmap suitable for upload.
    ///
    /// After calling this, the `MmapBuffer` is consumed and the temp file
    /// ownership transfers to the `MmapReadHandle`.
    pub fn into_read_handle(mut self) -> io::Result<MmapReadHandle> {
        // Flush any pending writes
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }

        // Drop the mutable mmap
        let _ = self.mmap.take();

        // Truncate file to actual length (optional optimization)
        self.file.as_file().set_len(self.len as u64)?;

        // Create read-only mmap
        // SAFETY: We own the file exclusively and just flushed/resized it.
        let mmap = unsafe { Mmap::map(self.file.as_file())? };

        Ok(MmapReadHandle {
            _file: self.file,
            mmap,
            len: self.len,
        })
    }
}

impl Write for MmapBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let required = self.len.saturating_add(buf.len());
        if required > self.capacity {
            self.grow(required)?;
        }

        if let Some(ref mut mmap) = self.mmap {
            let end = self.len + buf.len();
            mmap[self.len..end].copy_from_slice(buf);
            self.len = end;
            Ok(buf.len())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "mmap is None after grow",
            ))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }
        Ok(())
    }
}

/// Read-only handle to a completed mmap buffer.
///
/// This is returned by `MmapBuffer::into_read_handle()` and provides
/// efficient read access to the buffered data for upload.
///
/// The temp file is automatically deleted when this handle is dropped.
pub struct MmapReadHandle {
    /// Keeps the temp file alive (auto-deleted on drop).
    _file: NamedTempFile,
    /// Read-only memory map.
    mmap: Mmap,
    /// Number of valid bytes.
    len: usize,
}

impl MmapReadHandle {
    /// Returns the buffered data as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..self.len]
    }

    /// Returns the number of bytes in the buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AsRef<[u8]> for MmapReadHandle {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_write_read() {
        let mut buffer = MmapBuffer::new(1024).unwrap();
        buffer.write_all(b"hello world").unwrap();

        assert_eq!(buffer.len(), 11);
        assert_eq!(buffer.as_slice(), b"hello world");
    }

    #[test]
    fn test_multiple_writes() {
        let mut buffer = MmapBuffer::new(1024).unwrap();
        buffer.write_all(b"hello").unwrap();
        buffer.write_all(b" ").unwrap();
        buffer.write_all(b"world").unwrap();

        assert_eq!(buffer.len(), 11);
        assert_eq!(buffer.as_slice(), b"hello world");
    }

    #[test]
    fn test_growth() {
        // Start with small capacity
        let mut buffer = MmapBuffer::new(16).unwrap();

        // Write more than initial capacity
        let data = b"this is a much longer string that exceeds the initial 16 byte capacity";
        buffer.write_all(data).unwrap();

        assert_eq!(buffer.len(), data.len());
        assert!(buffer.capacity() >= data.len());
        assert_eq!(buffer.as_slice(), data.as_slice());
    }

    #[test]
    fn test_into_read_handle() {
        let mut buffer = MmapBuffer::new(1024).unwrap();
        buffer.write_all(b"test data for read handle").unwrap();

        let handle = buffer.into_read_handle().unwrap();

        assert_eq!(handle.len(), 25);
        assert_eq!(handle.as_slice(), b"test data for read handle");
        assert_eq!(handle.as_ref(), b"test data for read handle");
    }

    #[test]
    fn test_empty_buffer() {
        let buffer = MmapBuffer::new(1024).unwrap();

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.as_slice(), b"");

        let handle = buffer.into_read_handle().unwrap();
        assert_eq!(handle.len(), 0);
        assert!(handle.is_empty());
        assert_eq!(handle.as_slice(), b"");
    }

    #[test]
    fn test_large_write() {
        let mut buffer = MmapBuffer::new(1024).unwrap();

        // Write 10KB in one call
        let data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect();
        buffer.write_all(&data).unwrap();

        assert_eq!(buffer.len(), 10240);
        assert!(buffer.capacity() >= 10240);
        assert_eq!(buffer.as_slice(), data.as_slice());
    }

    #[test]
    fn test_flush() {
        let mut buffer = MmapBuffer::new(1024).unwrap();
        buffer.write_all(b"data to flush").unwrap();

        // Should not panic
        buffer.flush().unwrap();

        assert_eq!(buffer.as_slice(), b"data to flush");
    }

    #[test]
    fn test_default_capacity() {
        let buffer = MmapBuffer::with_default_capacity().unwrap();
        assert_eq!(buffer.capacity(), DEFAULT_INITIAL_CAPACITY);
    }

    #[test]
    fn test_zero_capacity_uses_default() {
        let buffer = MmapBuffer::new(0).unwrap();
        assert_eq!(buffer.capacity(), DEFAULT_INITIAL_CAPACITY);
    }
}
