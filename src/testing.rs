//! Test support: an in-memory S3 fake and helpers shared by unit tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Error, anyhow};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DeleteMarkerEntry, MetadataDirective, Object, ObjectVersion, StorageClass, Tag, Tier,
};
use clap::Parser;

use crate::adapters::aws::{
    DeleteObjectRequest, DeletedObjectInfo, ListObjectsRequest, ListVersionsRequest, ObjectsPage,
    RestoreObjectStatus, S3Client, VersionMarker, VersionsPage,
};
use crate::arg::{FindOpt, S3Path};
use crate::error::TagFetchError;
use crate::object::StreamObject;
use crate::run_command::{CommandContext, RunCommand};

/// Parses CLI arguments (without the program name) into options.
pub(crate) fn opts(args: &[&str]) -> FindOpt {
    FindOpt::try_parse_from(std::iter::once("s3find").chain(args.iter().copied()))
        .expect("valid test arguments")
}

/// Runs a command with an in-memory output sink and returns what it printed.
pub(crate) trait ExecuteCaptured {
    async fn execute_captured(
        &self,
        client: &impl S3Client,
        path: &S3Path,
        objects: &[StreamObject],
    ) -> Result<String, Error>;
}

impl<T: RunCommand> ExecuteCaptured for T {
    async fn execute_captured(
        &self,
        client: &impl S3Client,
        path: &S3Path,
        objects: &[StreamObject],
    ) -> Result<String, Error> {
        let mut out = Vec::new();
        let mut ctx = CommandContext {
            client,
            path,
            out: &mut out,
        };
        self.execute(&mut ctx, objects).await?;
        Ok(String::from_utf8(out)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ObjectRequestKey {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutTagCall {
    pub(crate) object: ObjectRequestKey,
    pub(crate) tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyCall {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) copy_source: String,
    pub(crate) storage_class: Option<String>,
    pub(crate) metadata_directive: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestoreCall {
    pub(crate) object: ObjectRequestKey,
    pub(crate) days: i32,
    pub(crate) tier: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListCall {
    pub(crate) prefix: String,
    pub(crate) delimiter: Option<String>,
    pub(crate) continuation_token: Option<String>,
}

/// An in-memory S3 for one or more buckets.
///
/// Listing follows S3 semantics (sorted keys, prefix, delimiter, max-keys and
/// continuation tokens); object operations are recorded so tests can assert
/// on them. Responses can be customised with the `with_*` builders.
pub(crate) struct FakeS3Client {
    pub(crate) region: Option<String>,
    pub(crate) objects: Mutex<BTreeMap<String, Object>>,
    pub(crate) version_pages: Mutex<Vec<(Vec<ObjectVersion>, Vec<DeleteMarkerEntry>)>>,
    pub(crate) list_error: Option<String>,
    pub(crate) list_calls: Mutex<Vec<ListCall>>,
    pub(crate) list_version_calls: Mutex<Vec<Option<VersionMarker>>>,
    pub(crate) delete_calls: Mutex<Vec<(String, Vec<DeleteObjectRequest>)>>,
    pub(crate) put_tag_calls: Mutex<Vec<PutTagCall>>,
    pub(crate) get_tag_calls: Mutex<Vec<ObjectRequestKey>>,
    pub(crate) public_calls: Mutex<Vec<ObjectRequestKey>>,
    pub(crate) get_object_calls: Mutex<Vec<ObjectRequestKey>>,
    pub(crate) copy_calls: Mutex<Vec<CopyCall>>,
    pub(crate) restore_calls: Mutex<Vec<RestoreCall>>,
    pub(crate) object_bodies: Mutex<HashMap<ObjectRequestKey, Vec<u8>>>,
    pub(crate) tag_outputs: Mutex<HashMap<ObjectRequestKey, Vec<Tag>>>,
    pub(crate) tag_errors: Mutex<HashMap<ObjectRequestKey, u16>>,
    pub(crate) tag_delays: Mutex<HashMap<ObjectRequestKey, Duration>>,
    pub(crate) restore_outputs: Mutex<HashMap<ObjectRequestKey, RestoreObjectStatus>>,
}

impl Default for FakeS3Client {
    fn default() -> Self {
        Self {
            region: Some("us-east-1".to_string()),
            objects: Mutex::default(),
            version_pages: Mutex::default(),
            list_error: None,
            list_calls: Mutex::default(),
            list_version_calls: Mutex::default(),
            delete_calls: Mutex::default(),
            put_tag_calls: Mutex::default(),
            get_tag_calls: Mutex::default(),
            public_calls: Mutex::default(),
            get_object_calls: Mutex::default(),
            copy_calls: Mutex::default(),
            restore_calls: Mutex::default(),
            object_bodies: Mutex::default(),
            tag_outputs: Mutex::default(),
            tag_errors: Mutex::default(),
            tag_delays: Mutex::default(),
            restore_outputs: Mutex::default(),
        }
    }
}

impl FakeS3Client {
    pub(crate) fn object_key(
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> ObjectRequestKey {
        ObjectRequestKey {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(str::to_string),
        }
    }

    /// Adds listable objects with the given keys and sizes.
    pub(crate) fn with_objects(self, objects: &[(&str, i64)]) -> Self {
        self.with_listed_objects(
            objects
                .iter()
                .map(|(key, size)| Object::builder().key(*key).size(*size).build()),
        )
    }

    /// Adds listable objects as built by the test.
    pub(crate) fn with_listed_objects(self, objects: impl IntoIterator<Item = Object>) -> Self {
        {
            let mut stored = self.objects.lock().unwrap();
            for object in objects {
                let key = object.key().expect("listed objects need a key").to_string();
                stored.insert(key, object);
            }
        }
        self
    }

    /// Serves these pages, in order, from ListObjectVersions.
    pub(crate) fn with_version_pages(
        self,
        pages: Vec<(Vec<ObjectVersion>, Vec<DeleteMarkerEntry>)>,
    ) -> Self {
        *self.version_pages.lock().unwrap() = pages;
        self
    }

    /// Makes every list call fail with this message.
    pub(crate) fn with_list_error(mut self, message: &str) -> Self {
        self.list_error = Some(message.to_string());
        self
    }

    pub(crate) fn with_object_body(
        self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        body: &[u8],
    ) -> Self {
        self.object_bodies
            .lock()
            .unwrap()
            .insert(Self::object_key(bucket, key, version_id), body.to_vec());
        self
    }

    pub(crate) fn with_tags(
        self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<Tag>,
    ) -> Self {
        self.tag_outputs
            .lock()
            .unwrap()
            .insert(Self::object_key(bucket, key, version_id), tags);
        self
    }

    /// Makes GetObjectTagging fail as S3 would with this HTTP status.
    pub(crate) fn with_tag_error(self, bucket: &str, key: &str, status: u16) -> Self {
        self.tag_errors
            .lock()
            .unwrap()
            .insert(Self::object_key(bucket, key, None), status);
        self
    }

    /// Delays GetObjectTagging for one key, to exercise concurrent ordering.
    pub(crate) fn with_tag_delay(self, bucket: &str, key: &str, millis: u64) -> Self {
        self.tag_delays.lock().unwrap().insert(
            Self::object_key(bucket, key, None),
            Duration::from_millis(millis),
        );
        self
    }

    pub(crate) fn with_restore_status(
        self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        status: RestoreObjectStatus,
    ) -> Self {
        self.restore_outputs
            .lock()
            .unwrap()
            .insert(Self::object_key(bucket, key, version_id), status);
        self
    }
}

/// The common prefix `key` rolls up into, if any: the key up to and including
/// the first delimiter after `prefix`.
fn common_prefix<'k>(key: &'k str, prefix: &str, delimiter: Option<&str>) -> Option<&'k str> {
    let delimiter = delimiter?;
    let rest = &key[prefix.len()..];
    rest.find(delimiter)
        .map(|index| &key[..prefix.len() + index + delimiter.len()])
}

impl S3Client for FakeS3Client {
    fn region(&self) -> Option<String> {
        self.region.clone()
    }

    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ObjectsPage, Error> {
        self.list_calls.lock().unwrap().push(ListCall {
            prefix: request.prefix.to_string(),
            delimiter: request.delimiter.map(str::to_string),
            continuation_token: request.continuation_token.map(str::to_string),
        });
        if let Some(message) = &self.list_error {
            return Err(anyhow!(message.clone()));
        }

        let max_keys = usize::try_from(request.max_keys).unwrap_or_default();
        let objects = self.objects.lock().unwrap();
        let mut page = ObjectsPage::default();
        let mut returned = 0;
        // The token is the last key or common prefix of the previous page.
        let mut last_entry = request.continuation_token.map(str::to_string);

        // Keys after the token; when the token is a common prefix, also skip
        // every key that rolled up into it.
        let token_is_prefix = request
            .continuation_token
            .zip(request.delimiter)
            .is_some_and(|(token, delimiter)| token.ends_with(delimiter));
        let after_token = objects.iter().filter(|(key, _)| {
            key.starts_with(request.prefix)
                && request.continuation_token.is_none_or(|token| {
                    key.as_str() > token && !(token_is_prefix && key.starts_with(token))
                })
        });

        for (key, object) in after_token {
            let entry = common_prefix(key, request.prefix, request.delimiter);
            if entry.is_some() && page.common_prefixes.last().map(String::as_str) == entry {
                continue;
            }
            if returned == max_keys {
                page.next_continuation_token = last_entry;
                break;
            }

            match entry {
                Some(prefix) => page.common_prefixes.push(prefix.to_string()),
                None => page.objects.push(object.clone()),
            }
            returned += 1;
            last_entry = Some(entry.unwrap_or(key).to_string());
        }

        Ok(page)
    }

    async fn list_object_versions(
        &self,
        request: ListVersionsRequest<'_>,
    ) -> Result<VersionsPage, Error> {
        self.list_version_calls
            .lock()
            .unwrap()
            .push(request.marker.cloned());
        if let Some(message) = &self.list_error {
            return Err(anyhow!(message.clone()));
        }

        let index: usize = request
            .marker
            .map(|marker| marker.key_marker.parse().expect("fake marker"))
            .unwrap_or(0);
        let pages = self.version_pages.lock().unwrap();
        let (versions, delete_markers) = pages.get(index).cloned().unwrap_or_default();

        Ok(VersionsPage {
            versions,
            delete_markers,
            next_marker: (index + 1 < pages.len()).then(|| VersionMarker {
                key_marker: (index + 1).to_string(),
                version_id_marker: None,
            }),
        })
    }

    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> Result<Vec<DeletedObjectInfo>, Error> {
        self.delete_calls
            .lock()
            .unwrap()
            .push((bucket.to_string(), objects.clone()));

        Ok(objects
            .into_iter()
            .map(|object| DeletedObjectInfo {
                key: Some(object.key),
                version_id: object.version_id,
            })
            .collect())
    }

    async fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<Tag>,
    ) -> Result<(), Error> {
        self.put_tag_calls.lock().unwrap().push(PutTagCall {
            object: Self::object_key(bucket, key, version_id),
            tags: tags
                .into_iter()
                .map(|tag| (tag.key().to_string(), tag.value().to_string()))
                .collect(),
        });
        Ok(())
    }

    async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Vec<Tag>, TagFetchError> {
        let object = Self::object_key(bucket, key, version_id);
        self.get_tag_calls.lock().unwrap().push(object.clone());

        let delay = self.tag_delays.lock().unwrap().get(&object).copied();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let error = self.tag_errors.lock().unwrap().get(&object).copied();
        match error {
            Some(403) => Err(TagFetchError::AccessDenied {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }),
            Some(404) => Err(TagFetchError::NotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }),
            Some(status) => Err(TagFetchError::ApiError(format!("HTTP {status}"))),
            None => Ok(self
                .tag_outputs
                .lock()
                .unwrap()
                .get(&object)
                .cloned()
                .unwrap_or_default()),
        }
    }

    async fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), Error> {
        self.public_calls
            .lock()
            .unwrap()
            .push(Self::object_key(bucket, key, version_id));
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ByteStream, Error> {
        let object = Self::object_key(bucket, key, version_id);
        self.get_object_calls.lock().unwrap().push(object.clone());
        let body = self
            .object_bodies
            .lock()
            .unwrap()
            .get(&object)
            .cloned()
            .ok_or_else(|| anyhow!("missing object body for {key}"))?;
        Ok(ByteStream::from(body))
    }

    async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> Result<(), Error> {
        self.copy_calls.lock().unwrap().push(CopyCall {
            bucket: bucket.to_string(),
            key: key.to_string(),
            copy_source: copy_source.to_string(),
            storage_class: storage_class.map(|value| value.as_str().to_string()),
            metadata_directive: metadata_directive.map(|value| value.as_str().to_string()),
        });
        Ok(())
    }

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        days: i32,
        tier: Tier,
    ) -> Result<RestoreObjectStatus, Error> {
        let object = Self::object_key(bucket, key, version_id);
        self.restore_calls.lock().unwrap().push(RestoreCall {
            object: object.clone(),
            days,
            tier: tier.as_str().to_string(),
        });

        Ok(self
            .restore_outputs
            .lock()
            .unwrap()
            .get(&object)
            .copied()
            .unwrap_or(RestoreObjectStatus::Started))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request<'a>(
        prefix: &'a str,
        delimiter: Option<&'a str>,
        token: Option<&'a str>,
        max_keys: i32,
    ) -> ListObjectsRequest<'a> {
        ListObjectsRequest {
            bucket: "bucket",
            prefix,
            delimiter,
            continuation_token: token,
            max_keys,
        }
    }

    fn keys(page: &ObjectsPage) -> Vec<&str> {
        page.objects.iter().map(|o| o.key().unwrap()).collect()
    }

    #[tokio::test]
    async fn fake_lists_with_prefix_and_pagination() {
        let client =
            FakeS3Client::default().with_objects(&[("a/1", 1), ("a/2", 1), ("a/3", 1), ("b/1", 1)]);

        let page = client
            .list_objects(request("a/", None, None, 2))
            .await
            .unwrap();
        assert_eq!(keys(&page), ["a/1", "a/2"]);
        assert_eq!(page.next_continuation_token.as_deref(), Some("a/2"));

        let page = client
            .list_objects(request("a/", None, Some("a/2"), 2))
            .await
            .unwrap();
        assert_eq!(keys(&page), ["a/3"]);
        assert_eq!(page.next_continuation_token, None);
    }

    #[tokio::test]
    async fn fake_groups_common_prefixes_across_pages() {
        let client = FakeS3Client::default().with_objects(&[
            ("root.txt", 1),
            ("x/1", 1),
            ("x/2", 1),
            ("y/1", 1),
        ]);

        let page = client
            .list_objects(request("", Some("/"), None, 2))
            .await
            .unwrap();
        assert_eq!(keys(&page), ["root.txt"]);
        assert_eq!(page.common_prefixes, ["x/"]);
        assert_eq!(page.next_continuation_token.as_deref(), Some("x/"));

        let page = client
            .list_objects(request("", Some("/"), Some("x/"), 2))
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.common_prefixes, ["y/"]);
        assert_eq!(page.next_continuation_token, None);
    }
}
