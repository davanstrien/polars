//! Hugging Face Hub integration for Polars.
//!
//! Provides read (glob expansion) and write (sink) support for HF Hub.

mod glob;
mod url;

// Re-export for use by path_utils
pub use glob::expand_paths_hf;
// Re-exported for future write support (Task 1.3+)
#[allow(unused_imports)]
pub(crate) use url::{HFPathParts, HFRepoLocation};

// Write support (gated by hf_sink feature)
#[cfg(feature = "hf_sink")]
pub mod auth;
#[cfg(feature = "hf_sink")]
pub mod error;
#[cfg(feature = "hf_sink")]
pub mod hashing_writer;
#[cfg(feature = "hf_sink")]
pub mod lfs;
#[cfg(feature = "hf_sink")]
pub mod mmap_buffer;
#[cfg(feature = "hf_sink")]
pub mod options;
#[cfg(feature = "hf_sink")]
pub mod shard_writer;

// Re-export auth function
#[cfg(feature = "hf_sink")]
pub use auth::get_hf_token;
// Re-export hashing writer
#[cfg(feature = "hf_sink")]
pub use hashing_writer::{HashingWriter, sha256_to_hex};
// Re-export mmap buffer
#[cfg(feature = "hf_sink")]
pub use mmap_buffer::{MmapBuffer, MmapReadHandle};
// Re-export shard writer
#[cfg(feature = "hf_sink")]
pub use shard_writer::{FinishedShard, HfShardWriter};
// Commit API client
#[cfg(feature = "hf_sink")]
pub mod commit;
#[cfg(feature = "hf_sink")]
pub use commit::{CommitOperation, CommitOperationAdd, CommitOperationDelete};
