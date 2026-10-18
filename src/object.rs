use std::fmt;

use aws_sdk_s3::types::{DeleteMarkerEntry, Object, ObjectStorageClass, ObjectVersion, Tag};

use crate::error::FunctionError;

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
        let ObjectVersion {
            key,
            size,
            last_modified,
            e_tag,
            storage_class,
            owner,
            version_id,
            is_latest,
            ..
        } = version;

        let object = Object::builder()
            .set_key(key)
            .set_size(size)
            .set_last_modified(last_modified)
            .set_e_tag(e_tag)
            .set_storage_class(storage_class.map(|class| ObjectStorageClass::from(class.as_str())))
            .set_owner(owner)
            .build();

        Self {
            object,
            version_id,
            is_latest,
            is_delete_marker: false,
            tags: None,
        }
    }

    /// Create from a DeleteMarkerEntry (versioned listing).
    pub fn from_delete_marker(marker: DeleteMarkerEntry) -> Self {
        let DeleteMarkerEntry {
            key,
            last_modified,
            owner,
            version_id,
            is_latest,
            ..
        } = marker;

        let object = Object::builder()
            .set_key(key)
            .size(0)
            .set_last_modified(last_modified)
            .set_owner(owner)
            .build();

        Self {
            object,
            version_id,
            is_latest,
            is_delete_marker: true,
            tags: None,
        }
    }

    /// Get the original key (without version info).
    #[inline]
    pub fn key(&self) -> Option<&str> {
        self.object.key()
    }

    /// Get the key, or an error for objects S3 returned without one.
    #[inline]
    pub fn require_key(&self) -> Result<&str, FunctionError> {
        self.key().ok_or(FunctionError::ObjectFieldError)
    }

    /// The key as shown in output, with version details when listed by
    /// version: `key?versionId=v1 (latest) (delete marker)`.
    #[inline]
    pub fn display_key(&self) -> DisplayKey<'_> {
        DisplayKey(self)
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
            .is_some_and(|tags| tags.iter().any(|t| t.key() == key))
    }
}

/// Formats an object's key with its version details, without allocating.
pub struct DisplayKey<'a>(&'a StreamObject);

impl fmt::Display for DisplayKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = self.0;
        f.write_str(object.key().unwrap_or_default())?;

        if let Some(version_id) = &object.version_id {
            write!(f, "?versionId={version_id}")?;
            if object.is_latest == Some(true) {
                f.write_str(" (latest)")?;
            }
            if object.is_delete_marker {
                f.write_str(" (delete marker)")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_require_key() {
        let stream_obj = StreamObject::from_object(Object::builder().key("a.txt").build());
        assert_eq!(stream_obj.require_key().unwrap(), "a.txt");

        let keyless = StreamObject::from_object(Object::builder().build());
        assert!(matches!(
            keyless.require_key(),
            Err(FunctionError::ObjectFieldError)
        ));
    }

    #[test]
    fn test_stream_object_from_version_with_version_id() {
        use aws_sdk_s3::types::ObjectVersionStorageClass;

        let version = ObjectVersion::builder()
            .key("test.txt")
            .version_id("abc123")
            .is_latest(true)
            .size(100)
            .storage_class(ObjectVersionStorageClass::Standard)
            .build();

        let stream_obj = StreamObject::from_version(version);

        // key() returns the original key
        assert_eq!(stream_obj.key(), Some("test.txt"));
        // Version info is in separate fields
        assert_eq!(stream_obj.version_id, Some("abc123".to_string()));
        assert_eq!(stream_obj.is_latest, Some(true));
        assert!(!stream_obj.is_delete_marker);
        // display_key() shows full version info
        assert_eq!(
            stream_obj.display_key().to_string(),
            "test.txt?versionId=abc123 (latest)"
        );
        assert_eq!(stream_obj.object.size(), Some(100));
    }

    #[test]
    fn test_stream_object_from_version_not_latest() {
        let version = ObjectVersion::builder()
            .key("test.txt")
            .version_id("old123")
            .is_latest(false)
            .size(50)
            .build();

        let stream_obj = StreamObject::from_version(version);

        // key() returns the original key
        assert_eq!(stream_obj.key(), Some("test.txt"));
        // Version info is in separate fields
        assert_eq!(stream_obj.version_id, Some("old123".to_string()));
        assert_eq!(stream_obj.is_latest, Some(false));
        // display_key() shows version but not (latest)
        assert_eq!(
            stream_obj.display_key().to_string(),
            "test.txt?versionId=old123"
        );
        assert_eq!(stream_obj.object.size(), Some(50));
    }

    #[test]
    fn test_stream_object_from_delete_marker() {
        let marker = DeleteMarkerEntry::builder()
            .key("deleted.txt")
            .version_id("del456")
            .is_latest(true)
            .build();

        let stream_obj = StreamObject::from_delete_marker(marker);

        // key() returns the original key
        assert_eq!(stream_obj.key(), Some("deleted.txt"));
        // Version info is in separate fields
        assert_eq!(stream_obj.version_id, Some("del456".to_string()));
        assert_eq!(stream_obj.is_latest, Some(true));
        assert!(stream_obj.is_delete_marker);
        // display_key() shows full info including delete marker
        assert_eq!(
            stream_obj.display_key().to_string(),
            "deleted.txt?versionId=del456 (latest) (delete marker)"
        );
        assert_eq!(stream_obj.object.size(), Some(0));
    }

    #[test]
    fn test_stream_object_from_object_non_versioned() {
        let object = Object::builder().key("simple.txt").size(200).build();

        let stream_obj = StreamObject::from_object(object);

        // key() returns the original key
        assert_eq!(stream_obj.key(), Some("simple.txt"));
        // No version info for non-versioned objects
        assert_eq!(stream_obj.version_id, None);
        assert_eq!(stream_obj.is_latest, None);
        assert!(!stream_obj.is_delete_marker);
        // display_key() is same as key for non-versioned
        assert_eq!(stream_obj.display_key().to_string(), "simple.txt");
        assert_eq!(stream_obj.object.size(), Some(200));
    }

    #[test]
    fn test_stream_object_has_tags() {
        let object = Object::builder().key("test.txt").build();
        let mut stream_obj = StreamObject::from_object(object);

        // Initially, tags are not fetched
        assert!(!stream_obj.has_tags());

        // After setting empty tags
        stream_obj.tags = Some(vec![]);
        assert!(stream_obj.has_tags());

        // After setting actual tags
        stream_obj.tags = Some(vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
        ]);
        assert!(stream_obj.has_tags());
    }

    #[test]
    fn test_stream_object_get_tag() {
        let object = Object::builder().key("test.txt").build();
        let mut stream_obj = StreamObject::from_object(object);

        // Tags not fetched
        assert_eq!(stream_obj.get_tag("env"), None);

        // Set tags
        stream_obj.tags = Some(vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ]);

        // Get existing tag
        assert_eq!(stream_obj.get_tag("env"), Some("prod"));
        assert_eq!(stream_obj.get_tag("team"), Some("data"));

        // Get non-existent tag
        assert_eq!(stream_obj.get_tag("nonexistent"), None);
    }

    #[test]
    fn test_stream_object_has_tag_key() {
        let object = Object::builder().key("test.txt").build();
        let mut stream_obj = StreamObject::from_object(object);

        // Tags not fetched - should return false
        assert!(!stream_obj.has_tag_key("env"));

        // Empty tags
        stream_obj.tags = Some(vec![]);
        assert!(!stream_obj.has_tag_key("env"));

        // Set tags
        stream_obj.tags = Some(vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ]);

        // Check existing keys
        assert!(stream_obj.has_tag_key("env"));
        assert!(stream_obj.has_tag_key("team"));

        // Check non-existent key
        assert!(!stream_obj.has_tag_key("nonexistent"));
    }

    #[test]
    fn test_stream_object_display_key_variants() {
        // Test all display_key() variants for better coverage
        let object = Object::builder().key("file.txt").build();

        // Non-versioned object
        let stream_obj = StreamObject {
            object: object.clone(),
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        };
        assert_eq!(stream_obj.display_key().to_string(), "file.txt");

        // Versioned object (not latest, not delete marker)
        let stream_obj = StreamObject {
            object: object.clone(),
            version_id: Some("v123".to_string()),
            is_latest: Some(false),
            is_delete_marker: false,
            tags: None,
        };
        assert_eq!(
            stream_obj.display_key().to_string(),
            "file.txt?versionId=v123"
        );

        // Latest version (not delete marker)
        let stream_obj = StreamObject {
            object: object.clone(),
            version_id: Some("v456".to_string()),
            is_latest: Some(true),
            is_delete_marker: false,
            tags: None,
        };
        assert_eq!(
            stream_obj.display_key().to_string(),
            "file.txt?versionId=v456 (latest)"
        );

        // Delete marker (not latest)
        let stream_obj = StreamObject {
            object: object.clone(),
            version_id: Some("v789".to_string()),
            is_latest: Some(false),
            is_delete_marker: true,
            tags: None,
        };
        assert_eq!(
            stream_obj.display_key().to_string(),
            "file.txt?versionId=v789 (delete marker)"
        );

        // Delete marker that is also latest
        let stream_obj = StreamObject {
            object: object.clone(),
            version_id: Some("v999".to_string()),
            is_latest: Some(true),
            is_delete_marker: true,
            tags: None,
        };
        assert_eq!(
            stream_obj.display_key().to_string(),
            "file.txt?versionId=v999 (latest) (delete marker)"
        );

        // Delete marker with no is_latest info
        let stream_obj = StreamObject {
            object,
            version_id: Some("v111".to_string()),
            is_latest: None,
            is_delete_marker: true,
            tags: None,
        };
        assert_eq!(
            stream_obj.display_key().to_string(),
            "file.txt?versionId=v111 (delete marker)"
        );
    }
}
