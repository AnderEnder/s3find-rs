use aws_sdk_s3::types::{DeleteMarkerEntry, Object, ObjectStorageClass, ObjectVersion, Tag};

/// Wrapper that carries version metadata alongside the S3 Object.
///
/// This keeps the original Object intact for filters while providing
/// version information for version-aware operations and display.
/// Also supports lazy-loaded tags for tag-based filtering.
#[derive(Debug, Clone)]
pub struct StreamObject {
    /// The S3 Object (contains key, size, last_modified, etc.)
    pub object: Object,
    /// Version ID if from versioned listing
    pub version_id: Option<String>,
    /// Whether this is the latest version
    pub is_latest: Option<bool>,
    /// Whether this is a delete marker
    pub is_delete_marker: bool,
    /// Cached object tags (None = not fetched, Some = fetched)
    /// Tags are lazy-loaded only when tag filtering is enabled.
    pub tags: Option<Vec<Tag>>,
}

impl StreamObject {
    /// Create from a regular Object (non-versioned listing).
    pub fn from_object(object: Object) -> Self {
        Self {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        }
    }

    /// Create from an ObjectVersion (versioned listing).
    pub fn from_version(version: ObjectVersion) -> Self {
        let object = Object::builder()
            .set_key(version.key.clone())
            .set_size(version.size)
            .set_last_modified(version.last_modified)
            .set_e_tag(version.e_tag.clone())
            .set_storage_class(
                version
                    .storage_class
                    .map(|sc| ObjectStorageClass::from(sc.as_str())),
            )
            .set_owner(version.owner.clone())
            .build();

        Self {
            object,
            version_id: version.version_id,
            is_latest: version.is_latest,
            is_delete_marker: false,
            tags: None,
        }
    }

    /// Create from a DeleteMarkerEntry (versioned listing).
    pub fn from_delete_marker(marker: DeleteMarkerEntry) -> Self {
        let object = Object::builder()
            .set_key(marker.key.clone())
            .size(0)
            .set_last_modified(marker.last_modified)
            .set_owner(marker.owner.clone())
            .build();

        Self {
            object,
            version_id: marker.version_id,
            is_latest: marker.is_latest,
            is_delete_marker: true,
            tags: None,
        }
    }

    /// Get the original key (without version info).
    #[inline]
    pub fn key(&self) -> Option<&str> {
        self.object.key()
    }

    /// Get key for display (includes version info if present).
    pub fn display_key(&self) -> String {
        let key = self.object.key().unwrap_or("");
        match (&self.version_id, self.is_latest, self.is_delete_marker) {
            (Some(vid), Some(true), true) => {
                format!("{}?versionId={} (latest) (delete marker)", key, vid)
            }
            (Some(vid), Some(true), false) => format!("{}?versionId={} (latest)", key, vid),
            (Some(vid), _, true) => format!("{}?versionId={} (delete marker)", key, vid),
            (Some(vid), _, false) => format!("{}?versionId={}", key, vid),
            (None, _, _) => key.to_string(),
        }
    }

    /// Check if tags have been fetched for this object.
    #[inline]
    pub fn has_tags(&self) -> bool {
        self.tags.is_some()
    }

    /// Get tag value by key.
    /// Returns None if tags haven't been fetched or if the key doesn't exist.
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags
            .as_ref()?
            .iter()
            .find(|t| t.key() == key)
            .map(|t| t.value())
    }

    /// Check if object has a tag with the given key (any value).
    /// Returns false if tags haven't been fetched.
    pub fn has_tag_key(&self, key: &str) -> bool {
        self.tags
            .as_ref()
            .map(|tags| tags.iter().any(|t| t.key() == key))
            .unwrap_or(false)
    }
}
