//! Dataset card (README.md) metadata support for HF Hub.
//!
//! Provides types for updating dataset card YAML frontmatter with split information.
//!
//! # README.md Format
//!
//! HF Hub dataset cards use YAML frontmatter delimited by `---`:
//!
//! ```text
//! ---
//! license: mit
//! dataset_info:
//!   splits:
//!   - name: train
//!     num_bytes: 1024000
//!     num_examples: 50000
//! ---
//!
//! # My Dataset
//!
//! Description here...
//! ```

use polars_error::{PolarsResult, polars_err};
use serde::{Deserialize, Serialize};

/// Information about a dataset split (e.g., "train", "test").
///
/// Used to update the `dataset_info.splits` section in HF dataset cards.
///
/// # Example
/// ```ignore
/// let split = SplitInfo::new("train", 1024 * 1024 * 100, 50000);
/// assert_eq!(split.name, "train");
/// assert_eq!(split.num_bytes, 104857600);
/// assert_eq!(split.num_examples, 50000);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SplitInfo {
    /// Split name (e.g., "train", "test", "validation")
    pub name: String,
    /// Total size in bytes
    pub num_bytes: u64,
    /// Total number of examples/rows
    pub num_examples: u64,
}

impl SplitInfo {
    /// Create a new SplitInfo.
    ///
    /// # Arguments
    /// * `name` - The split name (e.g., "train", "test")
    /// * `num_bytes` - Total size of the split in bytes
    /// * `num_examples` - Total number of rows/examples in the split
    pub fn new(name: impl Into<String>, num_bytes: u64, num_examples: u64) -> Self {
        Self {
            name: name.into(),
            num_bytes,
            num_examples,
        }
    }
}

/// Dataset info section from HF dataset card YAML.
///
/// This represents the `dataset_info` field in the YAML frontmatter.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DatasetInfo {
    /// Configuration name (e.g., "default")
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub config_name: Option<String>,
    /// List of splits with their metadata
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Vec::is_empty"))]
    pub splits: Vec<SplitInfo>,
    /// Total download size in bytes
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub download_size: Option<u64>,
    /// Total dataset size in bytes (sum of all splits)
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub dataset_size: Option<u64>,
}

impl DatasetInfo {
    /// Create a new DatasetInfo with the given splits.
    pub fn new(splits: Vec<SplitInfo>) -> Self {
        let dataset_size = if splits.is_empty() {
            None
        } else {
            Some(splits.iter().map(|s| s.num_bytes).sum())
        };
        Self {
            config_name: None,
            splits,
            download_size: None,
            dataset_size,
        }
    }

    /// Create a new DatasetInfo with a config name.
    pub fn with_config(mut self, config_name: impl Into<String>) -> Self {
        self.config_name = Some(config_name.into());
        self
    }

    /// Recalculate dataset_size from current splits.
    fn recalculate_dataset_size(&mut self) {
        self.dataset_size = if self.splits.is_empty() {
            None
        } else {
            Some(self.splits.iter().map(|s| s.num_bytes).sum())
        };
    }

    /// Update or add a split by name.
    ///
    /// If a split with the same name exists, it is replaced.
    /// Otherwise, the new split is appended.
    /// Automatically recalculates `dataset_size`.
    pub fn update_split(&mut self, split: SplitInfo) {
        if let Some(existing) = self.splits.iter_mut().find(|s| s.name == split.name) {
            *existing = split;
        } else {
            self.splits.push(split);
        }
        self.recalculate_dataset_size();
    }

    /// Update or add multiple splits by name.
    ///
    /// For each split, if a split with the same name exists, it is replaced.
    /// Otherwise, the new split is appended.
    /// Automatically recalculates `dataset_size` once after all updates.
    pub fn update_splits(&mut self, splits: impl IntoIterator<Item = SplitInfo>) {
        for split in splits {
            if let Some(existing) = self.splits.iter_mut().find(|s| s.name == split.name) {
                *existing = split;
            } else {
                self.splits.push(split);
            }
        }
        self.recalculate_dataset_size();
    }
}

/// Result of extracting YAML frontmatter from a README.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedFrontmatter<'a> {
    /// The YAML content (without the `---` delimiters)
    pub yaml: &'a str,
    /// The markdown body (after the closing `---`)
    pub body: &'a str,
}

/// Internal struct for parsing/serializing full README YAML frontmatter.
/// Uses `#[serde(flatten)]` to preserve unknown fields like license, task_categories, etc.
#[derive(Debug, Serialize, Deserialize)]
struct CardYaml {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dataset_info: Option<DatasetInfo>,
    #[serde(flatten)]
    other: serde_yaml::Mapping,
}

/// Extract YAML frontmatter from a README string.
///
/// Returns `None` if no valid frontmatter is found.
/// Frontmatter must start with `---` on the first line and end with `---`.
///
/// # Example
/// ```ignore
/// let readme = "---\nlicense: mit\n---\n\n# My Dataset";
/// let extracted = extract_frontmatter(readme).unwrap();
/// assert_eq!(extracted.yaml, "license: mit\n");
/// assert_eq!(extracted.body, "\n# My Dataset");
/// ```
pub fn extract_frontmatter(readme: &str) -> Option<ExtractedFrontmatter<'_>> {
    // Must start with `---` (optionally with trailing whitespace)
    let readme = readme.trim_start();
    if !readme.starts_with("---") {
        return None;
    }

    // Find the start of YAML content (after first `---` and newline)
    let after_opening = &readme[3..];
    let yaml_start = after_opening.find('\n').map(|i| i + 1)?;
    let yaml_content = &after_opening[yaml_start..];

    // Find the closing `---`
    let closing_pos = yaml_content.find("\n---")?;
    let yaml = &yaml_content[..closing_pos + 1]; // Include the newline before ---

    // Body starts after the closing `---` and its newline
    let body_start = closing_pos + 4; // "\n---".len()
    let body = if body_start < yaml_content.len() {
        let rest = &yaml_content[body_start..];
        // Skip the newline after closing ---
        if rest.starts_with('\n') {
            &rest[1..]
        } else {
            rest
        }
    } else {
        ""
    };

    Some(ExtractedFrontmatter { yaml, body })
}

/// Reconstruct README.md with updated dataset_info section.
///
/// Takes YAML frontmatter and body from `extract_frontmatter()`,
/// updates the `dataset_info` field, and reconstructs the README
/// with proper `---` delimiters.
///
/// # Arguments
/// * `yaml` - The YAML frontmatter (from ExtractedFrontmatter.yaml)
/// * `body` - The markdown body (from ExtractedFrontmatter.body)
/// * `updated_info` - The updated DatasetInfo to inject
///
/// # Returns
/// Complete README string with updated frontmatter
///
/// # Example
/// ```ignore
/// let extracted = extract_frontmatter(original_readme).unwrap();
/// let updated_readme = generate_updated_readme(
///     extracted.yaml,
///     extracted.body,
///     &updated_dataset_info
/// )?;
/// ```
pub fn generate_updated_readme(
    yaml: &str,
    body: &str,
    updated_info: &DatasetInfo,
) -> PolarsResult<String> {
    // 1. Parse existing YAML, preserving all other fields
    let mut card: CardYaml = serde_yaml::from_str(yaml)
        .map_err(|e| polars_err!(ComputeError: "Failed to parse YAML frontmatter: {}", e))?;

    // 2. Update dataset_info
    card.dataset_info = Some(updated_info.clone());

    // 3. Serialize back to YAML
    let yaml_str = serde_yaml::to_string(&card)
        .map_err(|e| polars_err!(ComputeError: "Failed to serialize YAML: {}", e))?;

    // 4. Reconstruct README with frontmatter delimiters
    // Note: serde_yaml already adds trailing newline
    Ok(format!("---\n{}---\n{}", yaml_str, body))
}

/// Generate a new README.md with only dataset_info metadata.
///
/// Use when no existing README exists in the repository.
///
/// # Example
/// ```ignore
/// let info = DatasetInfo::new(vec![
///     SplitInfo::new("train", 1024, 100),
/// ]);
/// let readme = generate_new_readme(&info)?;
/// ```
pub fn generate_new_readme(info: &DatasetInfo) -> PolarsResult<String> {
    // Create a CardYaml with just the dataset_info
    let card = CardYaml {
        dataset_info: Some(info.clone()),
        other: serde_yaml::Mapping::new(),
    };

    let yaml_str = serde_yaml::to_string(&card)
        .map_err(|e| polars_err!(ComputeError: "Failed to serialize YAML: {}", e))?;

    Ok(format!("---\n{}---\n", yaml_str))
}

/// Parse existing DatasetInfo from YAML frontmatter.
///
/// Used to extract and preserve existing split information when updating
/// the README.md. Returns None if parsing fails or no dataset_info exists.
///
/// # Arguments
/// * `yaml` - The YAML frontmatter content (without `---` delimiters)
///
/// # Example
/// ```ignore
/// let readme = "---\ndataset_info:\n  splits:\n  - name: train\n---\n";
/// let extracted = extract_frontmatter(readme).unwrap();
/// let info = parse_dataset_info_from_yaml(extracted.yaml);
/// assert!(info.is_some());
/// ```
pub fn parse_dataset_info_from_yaml(yaml: &str) -> Option<DatasetInfo> {
    serde_yaml::from_str::<CardYaml>(yaml)
        .ok()
        .and_then(|c| c.dataset_info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_info_new() {
        let split = SplitInfo::new("train", 1024, 100);
        assert_eq!(split.name, "train");
        assert_eq!(split.num_bytes, 1024);
        assert_eq!(split.num_examples, 100);
    }

    #[test]
    fn test_split_info_clone() {
        let split = SplitInfo::new("test", 2048, 200);
        let cloned = split.clone();
        assert_eq!(split, cloned);
    }

    #[test]
    fn test_split_info_debug() {
        let split = SplitInfo::new("validation", 512, 50);
        let debug_str = format!("{:?}", split);
        assert!(debug_str.contains("validation"));
        assert!(debug_str.contains("512"));
        assert!(debug_str.contains("50"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_split_info_serde_yaml() {
        let split = SplitInfo::new("train", 104857600, 50000);
        let yaml = serde_yaml::to_string(&split).unwrap();
        assert!(yaml.contains("name: train"));
        assert!(yaml.contains("num_bytes: 104857600"));
        assert!(yaml.contains("num_examples: 50000"));

        let deserialized: SplitInfo = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(split, deserialized);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_split_info_serde_json() {
        let split = SplitInfo::new("test", 2048, 100);
        let json = serde_json::to_string(&split).unwrap();
        let deserialized: SplitInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(split, deserialized);
    }

    // DatasetInfo tests
    #[test]
    fn test_dataset_info_new() {
        let splits = vec![
            SplitInfo::new("train", 1000, 100),
            SplitInfo::new("test", 500, 50),
        ];
        let info = DatasetInfo::new(splits);
        assert_eq!(info.splits.len(), 2);
        assert_eq!(info.dataset_size, Some(1500)); // 1000 + 500
        assert_eq!(info.config_name, None);
    }

    #[test]
    fn test_dataset_info_empty() {
        let info = DatasetInfo::new(vec![]);
        assert!(info.splits.is_empty());
        assert_eq!(info.dataset_size, None);
    }

    #[test]
    fn test_dataset_info_with_config() {
        let info = DatasetInfo::new(vec![SplitInfo::new("train", 1000, 100)])
            .with_config("default");
        assert_eq!(info.config_name, Some("default".to_string()));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_dataset_info_serde_yaml() {
        let info = DatasetInfo::new(vec![
            SplitInfo::new("train", 1000, 100),
        ]).with_config("default");

        let yaml = serde_yaml::to_string(&info).unwrap();
        assert!(yaml.contains("config_name: default"));
        assert!(yaml.contains("splits:"));
        assert!(yaml.contains("name: train"));

        let deserialized: DatasetInfo = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(info, deserialized);
    }

    // Frontmatter extraction tests
    #[test]
    fn test_extract_frontmatter_basic() {
        let readme = "---\nlicense: mit\n---\n\n# My Dataset";
        let extracted = extract_frontmatter(readme).unwrap();
        assert_eq!(extracted.yaml, "license: mit\n");
        assert_eq!(extracted.body, "\n# My Dataset");
    }

    #[test]
    fn test_extract_frontmatter_multiline_yaml() {
        let readme = "---\nlicense: mit\ntask_categories:\n  - text-classification\n---\n\n# Dataset";
        let extracted = extract_frontmatter(readme).unwrap();
        assert!(extracted.yaml.contains("license: mit"));
        assert!(extracted.yaml.contains("task_categories:"));
        assert_eq!(extracted.body, "\n# Dataset");
    }

    #[test]
    fn test_extract_frontmatter_no_frontmatter() {
        let readme = "# My Dataset\n\nNo frontmatter here.";
        assert!(extract_frontmatter(readme).is_none());
    }

    #[test]
    fn test_extract_frontmatter_unclosed() {
        let readme = "---\nlicense: mit\n# No closing delimiter";
        assert!(extract_frontmatter(readme).is_none());
    }

    #[test]
    fn test_extract_frontmatter_empty_yaml() {
        let readme = "---\n---\n\n# My Dataset";
        // Empty YAML between delimiters - still valid
        let extracted = extract_frontmatter(readme);
        // This should return None because there's no content before the closing ---
        assert!(extracted.is_none());
    }

    #[test]
    fn test_extract_frontmatter_with_leading_whitespace() {
        let readme = "  \n---\nlicense: mit\n---\n\nBody";
        let extracted = extract_frontmatter(readme).unwrap();
        assert_eq!(extracted.yaml, "license: mit\n");
    }

    #[test]
    fn test_extract_frontmatter_empty_body() {
        let readme = "---\nlicense: mit\n---";
        let extracted = extract_frontmatter(readme).unwrap();
        assert_eq!(extracted.yaml, "license: mit\n");
        assert_eq!(extracted.body, "");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_extract_and_parse_dataset_info() {
        let readme = r#"---
license: mit
dataset_info:
  config_name: default
  splits:
    - name: train
      num_bytes: 1024
      num_examples: 100
    - name: test
      num_bytes: 512
      num_examples: 50
---

# My Dataset
"#;
        let extracted = extract_frontmatter(readme).unwrap();

        // Parse the full YAML to get dataset_info
        #[derive(Deserialize)]
        struct CardYaml {
            dataset_info: Option<DatasetInfo>,
        }

        let card: CardYaml = serde_yaml::from_str(extracted.yaml).unwrap();
        let info = card.dataset_info.unwrap();

        assert_eq!(info.config_name, Some("default".to_string()));
        assert_eq!(info.splits.len(), 2);
        assert_eq!(info.splits[0].name, "train");
        assert_eq!(info.splits[0].num_bytes, 1024);
        assert_eq!(info.splits[1].name, "test");
    }

    // Update logic tests
    #[test]
    fn test_update_split_new() {
        let mut info = DatasetInfo::new(vec![SplitInfo::new("train", 1000, 100)]);
        info.update_split(SplitInfo::new("test", 500, 50));

        assert_eq!(info.splits.len(), 2);
        assert_eq!(info.splits[0].name, "train");
        assert_eq!(info.splits[1].name, "test");
        assert_eq!(info.splits[1].num_bytes, 500);
    }

    #[test]
    fn test_update_split_replace() {
        let mut info = DatasetInfo::new(vec![
            SplitInfo::new("train", 1000, 100),
            SplitInfo::new("test", 500, 50),
        ]);
        info.update_split(SplitInfo::new("train", 2000, 200));

        assert_eq!(info.splits.len(), 2);
        assert_eq!(info.splits[0].name, "train");
        assert_eq!(info.splits[0].num_bytes, 2000);
        assert_eq!(info.splits[0].num_examples, 200);
        // test split unchanged
        assert_eq!(info.splits[1].num_bytes, 500);
    }

    #[test]
    fn test_update_split_recalculates_size() {
        let mut info = DatasetInfo::new(vec![SplitInfo::new("train", 1000, 100)]);
        assert_eq!(info.dataset_size, Some(1000));

        info.update_split(SplitInfo::new("test", 500, 50));
        assert_eq!(info.dataset_size, Some(1500));

        info.update_split(SplitInfo::new("train", 2000, 200));
        assert_eq!(info.dataset_size, Some(2500)); // 2000 + 500
    }

    #[test]
    fn test_update_splits_multiple() {
        let mut info = DatasetInfo::new(vec![]);
        info.update_splits(vec![
            SplitInfo::new("train", 1000, 100),
            SplitInfo::new("test", 500, 50),
            SplitInfo::new("validation", 250, 25),
        ]);

        assert_eq!(info.splits.len(), 3);
        assert_eq!(info.dataset_size, Some(1750));
    }

    #[test]
    fn test_update_splits_mixed() {
        let mut info = DatasetInfo::new(vec![
            SplitInfo::new("train", 1000, 100),
            SplitInfo::new("test", 500, 50),
        ]);
        // Update train (existing) and add validation (new)
        info.update_splits(vec![
            SplitInfo::new("train", 3000, 300),
            SplitInfo::new("validation", 250, 25),
        ]);

        assert_eq!(info.splits.len(), 3);
        assert_eq!(info.splits[0].name, "train");
        assert_eq!(info.splits[0].num_bytes, 3000);
        assert_eq!(info.splits[1].name, "test");
        assert_eq!(info.splits[1].num_bytes, 500); // unchanged
        assert_eq!(info.splits[2].name, "validation");
        assert_eq!(info.dataset_size, Some(3750)); // 3000 + 500 + 250
    }

    #[test]
    fn test_update_split_preserves_config() {
        let mut info = DatasetInfo::new(vec![SplitInfo::new("train", 1000, 100)])
            .with_config("default");
        info.update_split(SplitInfo::new("test", 500, 50));

        assert_eq!(info.config_name, Some("default".to_string()));
        assert_eq!(info.splits.len(), 2);
    }

    // generate_updated_readme tests
    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_updated_readme_basic() {
        let readme = "---\nlicense: mit\n---\n\n# My Dataset";
        let extracted = extract_frontmatter(readme).unwrap();

        let info = DatasetInfo::new(vec![SplitInfo::new("train", 1024, 100)]);
        let updated = generate_updated_readme(extracted.yaml, extracted.body, &info).unwrap();

        // Should contain the frontmatter delimiters
        assert!(updated.starts_with("---\n"));
        assert!(updated.contains("---\n\n# My Dataset"));

        // Should contain the new dataset_info
        assert!(updated.contains("dataset_info:"));
        assert!(updated.contains("name: train"));
        assert!(updated.contains("num_bytes: 1024"));
        assert!(updated.contains("num_examples: 100"));

        // Should preserve the license
        assert!(updated.contains("license: mit"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_updated_readme_preserves_other_fields() {
        let readme = r#"---
license: apache-2.0
task_categories:
  - text-classification
language:
  - en
---

# My Dataset

Description here.
"#;
        let extracted = extract_frontmatter(readme).unwrap();
        let info = DatasetInfo::new(vec![SplitInfo::new("train", 2048, 200)]);
        let updated = generate_updated_readme(extracted.yaml, extracted.body, &info).unwrap();

        // Should preserve all original fields
        assert!(updated.contains("license:"));
        assert!(updated.contains("task_categories:"));
        assert!(updated.contains("text-classification"));
        assert!(updated.contains("language:"));

        // Should have the new dataset_info
        assert!(updated.contains("dataset_info:"));
        assert!(updated.contains("num_bytes: 2048"));

        // Should preserve the body
        assert!(updated.contains("# My Dataset"));
        assert!(updated.contains("Description here."));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_updated_readme_replaces_existing_dataset_info() {
        let readme = r#"---
license: mit
dataset_info:
  splits:
    - name: old_split
      num_bytes: 100
      num_examples: 10
---

# Dataset
"#;
        let extracted = extract_frontmatter(readme).unwrap();
        let info = DatasetInfo::new(vec![SplitInfo::new("new_split", 5000, 500)]);
        let updated = generate_updated_readme(extracted.yaml, extracted.body, &info).unwrap();

        // Should NOT contain the old split
        assert!(!updated.contains("old_split"));
        assert!(!updated.contains("num_bytes: 100"));

        // Should contain the new split
        assert!(updated.contains("new_split"));
        assert!(updated.contains("num_bytes: 5000"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_updated_readme_empty_body() {
        let readme = "---\nlicense: mit\n---";
        let extracted = extract_frontmatter(readme).unwrap();
        let info = DatasetInfo::new(vec![SplitInfo::new("train", 1024, 100)]);
        let updated = generate_updated_readme(extracted.yaml, extracted.body, &info).unwrap();

        assert!(updated.starts_with("---\n"));
        assert!(updated.contains("dataset_info:"));
        assert!(updated.ends_with("---\n"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_new_readme() {
        let info = DatasetInfo::new(vec![
            SplitInfo::new("train", 1024, 100),
            SplitInfo::new("test", 512, 50),
        ]);
        let readme = generate_new_readme(&info).unwrap();

        // Should have proper frontmatter structure
        assert!(readme.starts_with("---\n"));
        assert!(readme.ends_with("---\n"));

        // Should contain the dataset_info
        assert!(readme.contains("dataset_info:"));
        assert!(readme.contains("splits:"));
        assert!(readme.contains("name: train"));
        assert!(readme.contains("num_bytes: 1024"));
        assert!(readme.contains("name: test"));
        assert!(readme.contains("num_bytes: 512"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_generate_updated_readme_roundtrip() {
        // Test that we can extract → update → extract again
        let original = r#"---
license: mit
dataset_info:
  splits:
    - name: train
      num_bytes: 1000
      num_examples: 100
---

# Original Dataset
"#;
        let extracted1 = extract_frontmatter(original).unwrap();

        // Update with new info
        let mut info = DatasetInfo::new(vec![SplitInfo::new("train", 2000, 200)]);
        info.update_split(SplitInfo::new("test", 500, 50));

        let updated = generate_updated_readme(extracted1.yaml, extracted1.body, &info).unwrap();

        // Extract from updated README
        let extracted2 = extract_frontmatter(&updated).unwrap();

        // Parse and verify
        #[derive(serde::Deserialize)]
        struct CardYaml {
            dataset_info: Option<DatasetInfo>,
        }
        let card: CardYaml = serde_yaml::from_str(extracted2.yaml).unwrap();
        let parsed_info = card.dataset_info.unwrap();

        assert_eq!(parsed_info.splits.len(), 2);
        assert_eq!(parsed_info.splits[0].name, "train");
        assert_eq!(parsed_info.splits[0].num_bytes, 2000);
        assert_eq!(parsed_info.splits[1].name, "test");
        assert_eq!(parsed_info.splits[1].num_bytes, 500);
    }
}
