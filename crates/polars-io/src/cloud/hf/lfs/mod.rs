//! LFS (Large File Storage) protocol types and client.
//!
//! Implements the Git LFS Batch API used by HuggingFace Hub for file uploads.
//! Files uploaded via LFS are automatically migrated to Xet storage by HF Hub.

mod client;
mod types;
mod upload;

pub use client::LfsClient;
pub use types::{
    LfsAction, LfsActions, LfsBatchRequest, LfsBatchResponse, LfsMultipartCompleteRequest,
    LfsObject, LfsObjectError, LfsObjectRequest, LfsOperation, LfsPartCompletion, LfsPartInfo,
    LfsTransfer,
};
pub use upload::{NoOpProgress, UploadExecutor, UploadProgress};
