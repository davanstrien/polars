//! HF Hub shard writer.
//!
//! Combines Parquet encoding, SHA256 hashing, and temp file buffering into a
//! single component for writing shards to HF Hub.

use std::io::BufWriter;

use arrow::datatypes::ArrowSchema;
use arrow::record_batch::RecordBatch;
use polars_error::{PolarsResult, polars_err};
use polars_parquet::read::ParquetError;
use polars_parquet::write::{
    ColumnWriteOptions, Compressor, DynIter, DynStreamingIterator, FallibleStreamingIterator,
    FileWriter, SchemaDescriptor, WriteOptions, array_to_columns, to_parquet_schema,
};

use super::hashing_writer::{HashingWriter, sha256_to_hex};
use super::mmap_buffer::{MmapBuffer, MmapReadHandle};

/// Type alias for the nested writer chain.
type InnerWriter = BufWriter<HashingWriter<MmapBuffer>>;

/// Result of a completed shard write, ready for LFS upload.
///
/// Contains all the information needed to upload the shard to HF Hub:
/// - SHA256 hash (required by LFS API before upload)
/// - Size in bytes
/// - Row count for metadata
/// - Read handle for zero-copy upload
pub struct FinishedShard {
    /// SHA256 hash as lowercase hex string (64 characters).
    pub sha256: String,
    /// Total size in bytes.
    pub size: u64,
    /// Number of rows written.
    pub num_rows: usize,
    /// Read handle for upload (zero-copy access to data).
    pub buffer: MmapReadHandle,
}

/// Writer that encodes DataFrames to Parquet and buffers to temp file.
///
/// This component combines:
/// - `FileWriter`: Parquet encoding
/// - `BufWriter`: Batches small writes
/// - `HashingWriter`: Computes SHA256 incrementally
/// - `MmapBuffer`: Stores data in memory-mapped temp file
///
/// # Example
///
/// ```ignore
/// use polars_io::cloud::hf::HfShardWriter;
///
/// let mut writer = HfShardWriter::new(
///     schema,
///     500 * 1024 * 1024,  // 500MB initial capacity
///     write_options,
///     column_options,
/// )?;
///
/// writer.write_batch(batch1)?;
/// writer.write_batch(batch2)?;
///
/// let shard = writer.finish()?;
/// // shard.sha256, shard.size, shard.buffer ready for upload
/// ```
pub struct HfShardWriter {
    /// Parquet file writer (owns the writer chain).
    writer: FileWriter<InnerWriter>,
    /// Arrow schema for the data.
    arrow_schema: ArrowSchema,
    /// Parquet schema descriptor.
    parquet_schema: SchemaDescriptor,
    /// Per-column write options.
    column_options: Vec<ColumnWriteOptions>,
    /// Write options (compression, statistics, etc.).
    options: WriteOptions,
    /// Number of rows written so far.
    rows_written: usize,
}

impl HfShardWriter {
    /// Create a new shard writer for the given schema.
    ///
    /// # Arguments
    /// * `schema` - Arrow schema for the data
    /// * `initial_capacity` - Initial buffer capacity in bytes (typically max_shard_size)
    /// * `options` - Parquet write options (compression, statistics, version)
    /// * `column_options` - Per-column options (encoding, compression overrides)
    ///
    /// # Errors
    /// Returns an error if:
    /// - Buffer creation fails (disk space, permissions)
    /// - Schema conversion fails
    pub fn new(
        schema: ArrowSchema,
        initial_capacity: usize,
        options: WriteOptions,
        column_options: Vec<ColumnWriteOptions>,
    ) -> PolarsResult<Self> {
        // 1. Create MmapBuffer with initial capacity
        let buffer = MmapBuffer::new(initial_capacity)?;

        // 2. Wrap in HashingWriter for SHA256 computation
        let hashing = HashingWriter::new(buffer);

        // 3. Wrap in BufWriter for batched writes
        let buf_writer = BufWriter::new(hashing);

        // 4. Convert Arrow schema to Parquet schema
        let parquet_schema = to_parquet_schema(&schema, &column_options)?;

        // 5. Create FileWriter with the writer chain
        let writer = FileWriter::new_with_parquet_schema(
            buf_writer,
            schema.clone(),
            parquet_schema.clone(),
            options,
        );

        Ok(Self {
            writer,
            arrow_schema: schema,
            parquet_schema,
            column_options,
            options,
            rows_written: 0,
        })
    }

    /// Write a record batch as a parquet row group.
    ///
    /// Each call writes one row group to the parquet file. For best compression
    /// and performance, batches should be reasonably sized (e.g., 64K-256K rows).
    ///
    /// # Arguments
    /// * `batch` - Arrow RecordBatch to write
    ///
    /// # Errors
    /// Returns an error if encoding or writing fails.
    pub fn write_batch(&mut self, batch: RecordBatch) -> PolarsResult<()> {
        if batch.len() == 0 {
            return Ok(());
        }

        // Convert each column to compressed pages
        let columns = batch
            .columns()
            .iter()
            .zip(self.parquet_schema.fields())
            .zip(&self.column_options)
            .flat_map(|((array, type_), col_opts)| {
                // Encode array to parquet pages
                let encoded = array_to_columns(array, type_.clone(), col_opts, self.options)
                    .expect("array_to_columns should not fail for valid schema");

                // Compress pages
                encoded.into_iter().map(|pages| {
                    Ok(DynStreamingIterator::new(
                        Compressor::new_from_vec(
                            pages.map(|r| {
                                r.map_err(|e| {
                                    ParquetError::FeatureNotSupported(format!(
                                        "encoding error: {e}"
                                    ))
                                })
                            }),
                            self.options.compression,
                            vec![],
                        )
                        .map_err(polars_error::PolarsError::from),
                    ))
                })
            })
            .collect::<Vec<_>>();

        // Write row group
        let row_group = DynIter::new(columns.into_iter());
        self.writer.write(row_group)?;
        self.rows_written += batch.len();

        Ok(())
    }

    /// Returns the number of rows written so far.
    pub fn rows_written(&self) -> usize {
        self.rows_written
    }

    /// Returns the Arrow schema.
    pub fn schema(&self) -> &ArrowSchema {
        &self.arrow_schema
    }

    /// Returns the Parquet schema.
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        &self.parquet_schema
    }

    /// Finalize the shard and return upload-ready data.
    ///
    /// This method:
    /// 1. Writes the parquet footer
    /// 2. Flushes all buffers
    /// 3. Computes the final SHA256 hash
    /// 4. Returns a `FinishedShard` ready for LFS upload
    ///
    /// The writer is consumed by this method.
    ///
    /// # Errors
    /// Returns an error if flushing or finalizing fails.
    pub fn finish(mut self) -> PolarsResult<FinishedShard> {
        // 1. Write parquet footer (metadata, schema, etc.)
        self.writer.end(None, &self.column_options)?;

        // 2. Extract inner writer chain: FileWriter → BufWriter → HashingWriter → MmapBuffer
        let buf_writer: BufWriter<HashingWriter<MmapBuffer>> = self.writer.into_inner();

        // 3. Flush BufWriter and extract HashingWriter
        let hashing_writer = buf_writer
            .into_inner()
            .map_err(|e| polars_err!(ComputeError: "failed to flush buffer: {}", e.error()))?;

        // 4. Finish HashingWriter to get hash and MmapBuffer
        let (mmap_buffer, hash_bytes, size) = hashing_writer.finish();

        // 5. Convert to read handle for upload
        let read_handle = mmap_buffer.into_read_handle()?;

        // 6. Convert hash to hex string
        let sha256 = sha256_to_hex(&hash_bytes);

        Ok(FinishedShard {
            sha256,
            size,
            num_rows: self.rows_written,
            buffer: read_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, Utf8Array};
    use arrow::datatypes::{ArrowDataType, Field};
    use polars_parquet::write::{CompressionOptions, Version};

    use super::*;

    fn test_schema() -> ArrowSchema {
        ArrowSchema::from(vec![
            Field::new("id".into(), ArrowDataType::Int32, false),
            Field::new("name".into(), ArrowDataType::LargeUtf8, true),
        ])
    }

    fn default_write_options() -> WriteOptions {
        WriteOptions {
            statistics: polars_parquet::write::StatisticsOptions::empty(),
            compression: CompressionOptions::Uncompressed,
            version: Version::V1,
            data_page_size: None,
        }
    }

    fn default_column_options(schema: &ArrowSchema) -> Vec<ColumnWriteOptions> {
        schema
            .iter_values()
            .map(|_| ColumnWriteOptions::default())
            .collect()
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<Option<&str>> = (0..num_rows)
            .map(|i| if i % 2 == 0 { Some("test") } else { None })
            .collect();

        RecordBatch::try_new(
            Arc::new(test_schema()),
            vec![
                Arc::new(Int32Array::from_vec(ids)),
                Arc::new(Utf8Array::<i64>::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_writer() {
        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let writer = HfShardWriter::new(schema, 1024 * 1024, options, column_options);

        assert!(writer.is_ok());
        let writer = writer.unwrap();
        assert_eq!(writer.rows_written(), 0);
    }

    #[test]
    fn test_write_single_batch() {
        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let mut writer = HfShardWriter::new(schema, 1024 * 1024, options, column_options).unwrap();

        let batch = create_test_batch(100);
        writer.write_batch(batch).unwrap();

        assert_eq!(writer.rows_written(), 100);

        let shard = writer.finish().unwrap();
        assert_eq!(shard.num_rows, 100);
        assert_eq!(shard.sha256.len(), 64); // SHA256 hex is 64 chars
        assert!(shard.size > 0);
        assert!(!shard.buffer.is_empty());
    }

    #[test]
    fn test_write_multiple_batches() {
        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let mut writer = HfShardWriter::new(schema, 1024 * 1024, options, column_options).unwrap();

        writer.write_batch(create_test_batch(50)).unwrap();
        writer.write_batch(create_test_batch(75)).unwrap();
        writer.write_batch(create_test_batch(25)).unwrap();

        assert_eq!(writer.rows_written(), 150);

        let shard = writer.finish().unwrap();
        assert_eq!(shard.num_rows, 150);
    }

    #[test]
    fn test_empty_batch_ignored() {
        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let mut writer = HfShardWriter::new(schema, 1024 * 1024, options, column_options).unwrap();

        let empty_batch = create_test_batch(0);
        writer.write_batch(empty_batch).unwrap();

        assert_eq!(writer.rows_written(), 0);
    }

    #[test]
    fn test_finish_produces_valid_parquet() {
        use std::io::Cursor;

        use polars_parquet::read::FileReader;

        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let mut writer = HfShardWriter::new(schema, 1024 * 1024, options, column_options).unwrap();

        writer.write_batch(create_test_batch(100)).unwrap();

        let shard = writer.finish().unwrap();

        // Read back and verify it's valid parquet
        let data = shard.buffer.as_slice().to_vec();
        let cursor = Cursor::new(data);
        let reader = FileReader::try_new(cursor, None, None, None, None);
        assert!(reader.is_ok(), "Should produce valid parquet file");

        let reader = reader.unwrap();
        assert_eq!(reader.metadata().num_rows, 100);
    }

    #[test]
    fn test_sha256_consistency() {
        // Write the same data twice and verify SHA256 is identical
        let schema = test_schema();
        let options = default_write_options();
        let column_options = default_column_options(&schema);

        let create_and_finish = || {
            let mut writer =
                HfShardWriter::new(schema.clone(), 1024 * 1024, options, column_options.clone())
                    .unwrap();
            writer.write_batch(create_test_batch(100)).unwrap();
            writer.finish().unwrap()
        };

        let shard1 = create_and_finish();
        let shard2 = create_and_finish();

        assert_eq!(shard1.sha256, shard2.sha256);
        assert_eq!(shard1.size, shard2.size);
    }
}
