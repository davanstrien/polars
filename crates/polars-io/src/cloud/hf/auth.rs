//! HF Hub authentication and token handling.

use polars_core::config;
use polars_error::{PolarsResult, polars_bail};

use crate::path_utils::resolve_homedir;

/// Resolve HF Hub authentication token.
///
/// Priority order:
/// 1. Explicit token (if non-empty)
/// 2. HF_TOKEN environment variable
/// 3. Token file at HF_HOME/token (HF_HOME defaults to ~/.cache/huggingface)
///
/// Returns Ok(None) if no token found and required=false.
pub fn get_hf_token(explicit: Option<&str>, required: bool) -> PolarsResult<Option<String>> {
    let verbose = config::verbose();

    // 1. Check explicit token
    if let Some(token) = explicit {
        let token = token.trim();
        if !token.is_empty() {
            if verbose {
                eprintln!("HF token sourced from explicit parameter");
            }
            return Ok(Some(token.to_string()));
        }
    }

    // 2. Check HF_TOKEN env var
    if let Ok(token) = std::env::var("HF_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            if verbose {
                eprintln!("HF token sourced from HF_TOKEN env var");
            }
            return Ok(Some(token.to_string()));
        }
    }

    // 3. Check token file at HF_HOME/token
    let hf_home = std::env::var("HF_HOME");
    let hf_home = hf_home.as_deref().unwrap_or("~/.cache/huggingface");
    let hf_home = resolve_homedir(hf_home);
    let token_path = hf_home.join("token");

    if let Ok(bytes) = std::fs::read(&token_path) {
        if let Ok(token) = String::from_utf8(bytes) {
            let token = token.trim();
            if !token.is_empty() {
                if verbose {
                    eprintln!("HF token sourced from {:?}", token_path);
                }
                return Ok(Some(token.to_string()));
            }
        }
    }

    // No token found
    if required {
        polars_bail!(InvalidOperation:
            "HF Hub token required but not found. \
             Set HF_TOKEN environment variable or run `huggingface-cli login`"
        );
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_token() {
        let result = get_hf_token(Some("hf_test_token"), false).unwrap();
        assert_eq!(result, Some("hf_test_token".to_string()));
    }

    #[test]
    fn test_explicit_token_trimmed() {
        let result = get_hf_token(Some("  hf_test_token  "), false).unwrap();
        assert_eq!(result, Some("hf_test_token".to_string()));
    }

    #[test]
    fn test_empty_explicit_not_used() {
        // Empty explicit should fall through (returns None if no env/file)
        let result = get_hf_token(Some(""), false).unwrap();
        // Will be None unless HF_TOKEN env is set or token file exists
        // Just verify it doesn't error
        let _ = result;
    }

    #[test]
    fn test_not_required_returns_none() {
        // With no token sources, should return Ok(None)
        let result = get_hf_token(None, false).unwrap();
        // May be Some if HF_TOKEN is set in env, but shouldn't error
        let _ = result;
    }

    #[test]
    fn test_required_with_explicit_token_ok() {
        // When required=true but we have explicit token, should succeed
        let result = get_hf_token(Some("valid_token"), true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("valid_token".to_string()));
    }
}
