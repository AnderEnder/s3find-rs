use std::fmt;
use std::ops::Add;
use std::pin::Pin;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};
use aws_sdk_s3::types::{DeleteMarkerEntry, Object, ObjectStorageClass, ObjectVersion, Tag};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use aws_smithy_runtime_api::http::Response;

use futures::Stream;
use futures::StreamExt;
use humansize::*;

use crate::arg::*;
use crate::run_command::*;

// Type aliases for cleaner signatures
type BoxedStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
type S3Result<T> = Result<T, SdkError<ListObjectsV2Error, Response>>;

// Delimiter used for hierarchical S3 listing
const S3_PATH_DELIMITER: &str = "/";

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
            // Convert ObjectVersionStorageClass to ObjectStorageClass via string.
            // ObjectStorageClass::from(&str) handles unknown values by creating
            // an Unknown variant, so this is safe even if new storage classes are added.
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

pub struct FindCommand {
    pub client: Client,
    pub path: S3Path,
    pub command: Box<dyn RunCommand>,
}

impl FindCommand {
    pub fn new(cmd: Option<Cmd>, path: S3Path, client: Client) -> Self {
        let command = cmd.unwrap_or_default().downcast();

        FindCommand {
            client,
            path,
            command,
        }
    }

    pub async fn exec(&self, acc: Option<FindStat>, list: Vec<StreamObject>) -> Option<FindStat> {
        // Extract inner objects for stats calculation
        let objects: Vec<Object> = list.iter().map(|so| so.object.clone()).collect();
        let status = acc.map(|stat| stat + &objects);

        self.command
            .execute(&self.client, &self.path, &list)
            .await
            .unwrap();
        status
    }

    pub fn from_opts(opts: &FindOpt, client: Client) -> FindCommand {
        let FindOpt { path, cmd, .. } = opts;

        let path = S3Path { ..path.clone() };

        FindCommand::new(cmd.clone(), path, client)
    }
}

pub fn default_stats(summarize: bool) -> Option<FindStat> {
    if summarize {
        Some(FindStat::default())
    } else {
        None
    }
}

pub struct FindStream {
    pub client: Client,
    pub path: S3Path,
    pub token: Option<String>,
    pub page_size: i64,
    pub initial: bool,
    pub maxdepth: Option<usize>,
    pub all_versions: bool,
}

impl FindStream {
    pub fn from_opts(opts: &FindOpt, client: Client) -> Self {
        let path = opts.path.clone();

        FindStream {
            client,
            path,
            token: None,
            page_size: opts.page_size,
            initial: true,
            maxdepth: opts.maxdepth,
            all_versions: opts.all_versions,
        }
    }

    async fn paginator(
        self,
    ) -> PaginationStream<Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, Response>>> {
        self.client
            .list_objects_v2()
            .bucket(self.path.bucket.clone())
            .prefix(self.path.prefix.clone().unwrap_or_else(|| "".to_owned()))
            .max_keys(self.page_size as i32)
            .into_paginator()
            .send()
    }

    /// Recursively streams S3 objects up to maxdepth using delimiter-based traversal.
    ///
    /// This function uses S3's delimiter parameter to efficiently traverse the object
    /// hierarchy server-side, avoiding the need to fetch objects beyond the specified depth.
    ///
    /// # Arguments
    ///
    /// * `client` - AWS S3 client reference
    /// * `bucket` - S3 bucket name (borrowed to avoid cloning in recursion)
    /// * `prefix` - Starting prefix to search from
    /// * `maxdepth` - Maximum recursion depth:
    ///   - `0` = objects at prefix level only (no subdirectories)
    ///   - `1` = prefix level + one subdirectory level
    ///   - `n` = prefix level + n subdirectory levels
    /// * `current_depth` - Current recursion level (starts at 0)
    /// * `page_size` - Number of results per S3 API call
    ///
    /// # Returns
    ///
    /// A stream of `Result<StreamObject, SdkError>` that yields objects immediately as they're
    /// fetched, without accumulating them in memory. Errors are propagated through the stream.
    ///
    /// # Performance
    ///
    /// - Objects are streamed immediately (no memory accumulation)
    /// - Only fetches objects up to maxdepth from S3 (server-side filtering)
    /// - Subdirectories are traversed sequentially to maintain order
    ///
    /// # Error Handling
    ///
    /// Errors from S3 API calls are yielded through the stream and stop further traversal.
    /// When an error occurs, it is yielded to the consumer and the stream terminates.
    fn collect_objects_recursive<'a>(
        client: &'a Client,
        bucket: &'a str,
        prefix: String,
        maxdepth: usize,
        current_depth: usize,
        page_size: i32,
    ) -> BoxedStream<'a, S3Result<StreamObject>> {
        Box::pin(async_stream::stream! {
            // Special case: maxdepth=0 means no recursion at all
            if current_depth > maxdepth {
                return;
            }

            // List objects at current level with delimiter for hierarchical traversal
            let mut paginator = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix.clone())
                .delimiter(S3_PATH_DELIMITER)
                .max_keys(page_size)
                .into_paginator()
                .send();

            while let Some(result) = paginator.next().await {
                match result {
                    Ok(output) => {
                        // Yield objects at this level immediately (no accumulation)
                        if let Some(contents) = output.contents {
                            for obj in contents {
                                yield Ok(StreamObject::from_object(obj));
                            }
                        }

                        // Recurse into subdirectories if within depth limit
                        if current_depth < maxdepth
                            && let Some(common_prefixes) = output.common_prefixes
                        {
                            for common_prefix in common_prefixes {
                                if let Some(prefix_str) = common_prefix.prefix {
                                    // Recursively stream objects from subdirectory
                                    let mut sub_stream = Self::collect_objects_recursive(
                                        client,
                                        bucket,  // No clone needed - just borrow
                                        prefix_str,
                                        maxdepth,
                                        current_depth + 1,
                                        page_size,
                                    );

                                    // Yield all objects from subdirectory stream
                                    while let Some(item) = sub_stream.next().await {
                                        yield item;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Propagate error through stream
                        yield Err(e);
                        break;
                    }
                }
            }
        })
    }

    /// Creates a stream with depth-limited traversal using S3 delimiter parameter.
    ///
    /// Returns objects in batches (Vec<StreamObject>) to maintain consistency with the
    /// standard pagination interface. Objects are streamed from S3 and batched
    /// client-side, avoiding memory accumulation.
    fn paginator_with_depth(self) -> BoxedStream<'static, Vec<StreamObject>> {
        let maxdepth = self.maxdepth.unwrap_or(usize::MAX);
        let base_prefix = self.path.prefix.clone().unwrap_or_else(|| "".to_owned());
        let bucket = self.path.bucket.clone();
        let page_size = self.page_size;

        Box::pin(async_stream::stream! {
            let obj_stream = Self::collect_objects_recursive(
                &self.client,
                &bucket,
                base_prefix,
                maxdepth,
                0,
                page_size as i32,
            );

            futures::pin_mut!(obj_stream);

            let mut chunk = Vec::with_capacity(page_size as usize);

            while let Some(result) = obj_stream.next().await {
                match result {
                    Ok(obj) => {
                        chunk.push(obj);
                        // Yield chunk when it reaches page_size
                        if chunk.len() >= page_size as usize {
                            yield std::mem::take(&mut chunk);
                            chunk = Vec::with_capacity(page_size as usize);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error listing objects: {:?}", e);
                        // Yield partial chunk before stopping
                        if !chunk.is_empty() {
                            yield std::mem::take(&mut chunk);
                        }
                        break;
                    }
                }
            }

            // Yield remaining objects in final partial chunk
            if !chunk.is_empty() {
                yield chunk;
            }
        })
    }

    /// Creates a stream of object versions using ListObjectVersions API.
    ///
    /// This method lists all versions of objects, including delete markers.
    /// Each version is wrapped in a StreamObject with version metadata preserved.
    fn versions_paginator(self) -> BoxedStream<'static, Vec<StreamObject>> {
        let bucket = self.path.bucket.clone();
        let prefix = self.path.prefix.clone().unwrap_or_default();
        let page_size = self.page_size;

        Box::pin(async_stream::stream! {
            let mut key_marker: Option<String> = None;
            let mut version_id_marker: Option<String> = None;

            loop {
                let mut request = self.client
                    .list_object_versions()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .max_keys(page_size as i32);

                if let Some(ref km) = key_marker {
                    request = request.key_marker(km);
                }
                if let Some(ref vim) = version_id_marker {
                    request = request.version_id_marker(vim);
                }

                match request.send().await {
                    Ok(output) => {
                        let mut stream_objects = Vec::new();

                        // Convert ObjectVersions to StreamObjects
                        if let Some(versions) = output.versions {
                            for version in versions {
                                stream_objects.push(StreamObject::from_version(version));
                            }
                        }

                        // Include delete markers as well
                        if let Some(markers) = output.delete_markers {
                            for marker in markers {
                                stream_objects.push(StreamObject::from_delete_marker(marker));
                            }
                        }

                        // Sort by key, then by last_modified descending to preserve S3's natural order
                        // (versions and delete markers are interleaved by key in S3 API responses)
                        stream_objects.sort_by(|a, b| {
                            let key_cmp = a.object.key().cmp(&b.object.key());
                            if key_cmp != std::cmp::Ordering::Equal {
                                return key_cmp;
                            }
                            // Within same key, sort by last_modified descending (newest first)
                            b.object.last_modified().cmp(&a.object.last_modified())
                        });

                        if !stream_objects.is_empty() {
                            yield stream_objects;
                        }

                        // Check if there are more results
                        if output.is_truncated.unwrap_or(false) {
                            key_marker = output.next_key_marker;
                            version_id_marker = output.next_version_id_marker;
                        } else {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error listing object versions: {:?}", e);
                        break;
                    }
                }
            }
        })
    }

    /// Creates a stream of S3 objects, using delimiter-based depth limiting if maxdepth is set.
    ///
    /// This method is non-async and returns a stream immediately without starting any I/O,
    /// enabling lazy evaluation. The stream begins fetching data only when polled.
    ///
    /// # Returns
    ///
    /// A pinned, boxed stream that yields batches of stream objects (`Vec<StreamObject>`).
    /// - If `all_versions` is set: Uses ListObjectVersions API (ignores maxdepth)
    /// - If `maxdepth` is set: Uses delimiter-based hierarchical traversal
    /// - Otherwise: Uses standard flat pagination
    ///
    /// # Note
    ///
    /// When both `all_versions` and `maxdepth` are set, `all_versions` takes precedence
    /// and `maxdepth` is ignored. A warning is printed to stderr in this case.
    pub fn stream(self) -> BoxedStream<'static, Vec<StreamObject>> {
        // Use version listing if all_versions is set
        if self.all_versions {
            // Warn if maxdepth is also set (it will be ignored)
            if self.maxdepth.is_some() {
                eprintln!("Warning: --maxdepth is ignored when --all-versions is used");
            }
            return self.versions_paginator();
        }

        // Use delimiter-based traversal if maxdepth is set
        if self.maxdepth.is_some() {
            return self.paginator_with_depth();
        }

        // Otherwise use standard pagination
        Box::pin(async_stream::stream! {
            let mut ps = self.paginator().await;

            while let Some(result) = ps.next().await {
                match result {
                    Ok(output) => {
                        let objects = output.contents.unwrap_or_default();
                        if !objects.is_empty() {
                            // Wrap objects in StreamObject
                            let stream_objects: Vec<StreamObject> = objects
                                .into_iter()
                                .map(StreamObject::from_object)
                                .collect();
                            yield stream_objects;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error listing objects: {:?}", e);
                        break;
                    }
                }
            }
        })
    }
}

impl PartialEq for FindStream {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.token == other.token
            && self.page_size == other.page_size
            && self.initial == other.initial
            && self.maxdepth == other.maxdepth
            && self.all_versions == other.all_versions
    }
}

impl fmt::Debug for FindStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\
FindStream {{
    client,
    path: {:?},
    token: {:?},
    page_size: {},
    initial: {},
    maxdepth: {:?},
    all_versions: {},
}}",
            self.path, self.token, self.page_size, self.initial, self.maxdepth, self.all_versions
        )
    }
}

impl fmt::Display for FindStat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let file_size = make_format(BINARY);
        writeln!(f)?;
        writeln!(f, "Summary")?;
        writeln!(f, "{:19} {}", "Total files:", &self.total_files)?;
        writeln!(
            f,
            "Total space:        {}",
            file_size(self.total_space as u64),
        )?;
        writeln!(f, "{:19} {}", "Largest file:", &self.max_key)?;
        writeln!(
            f,
            "{:19} {}",
            "Largest file size:",
            file_size(self.max_size.unwrap_or_default() as u64),
        )?;
        writeln!(f, "{:19} {}", "Smallest file:", &self.min_key)?;
        writeln!(f, "{:19} {}", "Smallest file size:", self.min_key,)?;
        writeln!(
            f,
            "{:19} {}",
            "Average file size:",
            file_size(self.average_size as u64),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FindStat {
    pub total_files: usize,
    pub total_space: i64,
    pub max_size: Option<i64>,
    pub min_size: Option<i64>,
    pub max_key: String,
    pub min_key: String,
    pub average_size: i64,
}

impl Add<&[Object]> for FindStat {
    type Output = FindStat;

    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self: FindStat, list: &[Object]) -> Self {
        for x in list {
            self.total_files += 1;
            let size = x.size;
            self.total_space += size.unwrap_or_default();

            match self.max_size {
                None => {
                    self.max_size = size;
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                Some(max_size) if max_size <= size.unwrap_or_default() => {
                    self.max_size = size;
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            match self.min_size {
                None => {
                    self.min_size = size;
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                Some(min_size) if min_size > size.unwrap_or_default() => {
                    self.min_size = size;
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            self.average_size = self.total_space / (self.total_files as i64);
        }
        self
    }
}

impl Default for FindStat {
    fn default() -> Self {
        FindStat {
            total_files: 0,
            total_space: 0,
            max_size: None,
            min_size: None,
            max_key: "".to_owned(),
            min_key: "".to_owned(),
            average_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::adapters::aws::setup_client;

    use super::*;
    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_s3::{config::Credentials, types::ObjectStorageClass};
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use futures::StreamExt;
    use glob::Pattern;
    use http::{HeaderValue, StatusCode};
    use regex::Regex;

    #[test]
    fn test_find_stat_display() {
        let find_stat = FindStat {
            total_files: 42,
            total_space: 1234567890,
            max_size: Some(987654321),
            min_size: Some(123),
            max_key: "largest-file.txt".to_owned(),
            min_key: "smallest-file.txt".to_owned(),
            average_size: 29394474,
        };

        let display_output = find_stat.to_string();

        assert!(display_output.contains("Summary"), "Missing Summary header");
        assert!(
            display_output.contains("Total files:        42"),
            "Incorrect total files count"
        );
        assert!(
            display_output.contains("Total space:"),
            "Missing total space"
        );
        assert!(
            display_output.contains("Largest file:       largest-file.txt"),
            "Incorrect largest file name"
        );
        assert!(
            display_output.contains("Largest file size:"),
            "Missing largest file size"
        );
        assert!(
            display_output.contains("Smallest file:      smallest-file.txt"),
            "Incorrect smallest file name"
        );
        assert!(
            display_output.contains("Smallest file size:"),
            "Missing smallest file size"
        );
        assert!(
            display_output.contains("Average file size:"),
            "Missing average file size"
        );

        assert!(
            display_output.contains("GiB"),
            "Expected binary size format (MiB/GiB)"
        );
    }

    #[test]
    fn test_find_stat_add() {
        let mut stat = FindStat::default();

        let objects = vec![
            Object::builder().key("file1.txt").size(100).build(),
            Object::builder().key("file2.txt").size(200).build(),
            Object::builder().key("file3.txt").size(50).build(),
        ];

        stat = stat + &objects;

        assert_eq!(stat.total_files, 3, "Total files count incorrect");
        assert_eq!(stat.total_space, 350, "Total space incorrect");
        assert_eq!(stat.max_size, Some(200), "Max size incorrect");
        assert_eq!(stat.max_key, "file2.txt", "Max key incorrect");
        assert_eq!(stat.min_size, Some(50), "Min size incorrect");
        assert_eq!(stat.min_key, "file3.txt", "Min key incorrect");
        assert_eq!(stat.average_size, 116, "Average size incorrect");

        let more_objects = vec![
            Object::builder().key("file4.txt").size(500).build(),
            Object::builder().key("file5.txt").size(10).build(),
        ];

        stat = stat + &more_objects;

        assert_eq!(
            stat.total_files, 5,
            "Total files count incorrect after second add"
        );
        assert_eq!(
            stat.total_space, 860,
            "Total space incorrect after second add"
        );
        assert_eq!(
            stat.max_size,
            Some(500),
            "Max size incorrect after second add"
        );
        assert_eq!(
            stat.max_key, "file4.txt",
            "Max key incorrect after second add"
        );
        assert_eq!(
            stat.min_size,
            Some(10),
            "Min size incorrect after second add"
        );
        assert_eq!(
            stat.min_key, "file5.txt",
            "Min key incorrect after second add"
        );
        assert_eq!(
            stat.average_size, 172,
            "Average size incorrect after second add"
        );

        let object_without_size = vec![Object::builder().key("no-size.txt").build()];

        let before_total_space = stat.total_space;
        stat = stat + &object_without_size;

        assert_eq!(
            stat.total_files, 6,
            "Total files should increase even for objects with no size"
        );
        assert_eq!(
            stat.total_space, before_total_space,
            "Total space shouldn't change for object with no size"
        );

        let empty_list: Vec<Object> = vec![];
        let before = stat.clone();
        stat = stat + &empty_list;

        assert_eq!(stat, before, "Adding empty list should not change stats");
    }

    #[tokio::test]
    async fn test_find_exec() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        };

        let command = DoNothing {};

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);

        let find = FindCommand {
            client,
            path,
            command: Box::new(command),
        };

        // Create test objects wrapped in StreamObject
        let stream_objects = vec![
            StreamObject::from_object(Object::builder().key("object1").size(100).build()),
            StreamObject::from_object(Object::builder().key("object2").size(200).build()),
        ];

        // Execute find with stats
        let acc = Some(FindStat::default());
        let result = find.exec(acc, stream_objects).await;

        // Verify stats were updated
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.total_files, 2);
        assert_eq!(stats.total_space, 300);
    }

    #[tokio::test]
    async fn test_find_stream_list() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);

        let find_stream = FindStream {
            client: client.clone(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        assert_eq!(find_stream.token, None);
        assert_eq!(find_stream.page_size, 1000);
        assert!(find_stream.initial);
        assert_eq!(find_stream.path, path);

        let same_stream = FindStream {
            client: client.clone(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        assert_eq!(find_stream, same_stream);

        let different_stream = FindStream {
            client,
            path,
            token: Some("token".to_string()),
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        assert_ne!(find_stream, different_stream);
    }

    #[tokio::test]
    async fn test_find_stream_stream_compile() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let _stream = find_stream.stream(); // Non-async, returns stream immediately
    }

    #[test]
    fn test_find_stream_debug() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-west-2"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let find_stream = FindStream {
            client: Client::from_conf(config),
            path: path.clone(),
            token: Some("test-token".to_string()),
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let debug_str = format!("{:?}", find_stream);

        assert!(debug_str.contains("FindStream {"));
        assert!(debug_str.contains(&format!("path: {:?}", path)));
        assert!(debug_str.contains("token: Some(\"test-token\")"));
        assert!(debug_str.contains("page_size: 1000"));
        assert!(debug_str.contains("initial: true"));
        assert!(debug_str.contains("maxdepth: None"));
        assert!(debug_str.contains("all_versions: false"));
    }

    #[tokio::test]
    async fn test_find_new() {
        let args = FindOpt {
            aws_access_key: Some("mock_access".to_string()),
            aws_secret_key: Some("mock_secret".to_string()),
            aws_region: Some(Region::new("mock-region")),
            endpoint_url: None,
            force_path_style: false,
            path: S3Path {
                bucket: "test-bucket".to_string(),
                prefix: Some("test-prefix/".to_string()),
            },
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            cmd: None,
            storage_class: None,
            page_size: 500,
            summarize: true,
            limit: Some(100),
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };
        let client = setup_client(&args).await;

        let find = FindCommand::new(args.cmd, args.path.clone(), client.clone());

        assert_eq!(find.path.bucket, "test-bucket");
        assert_eq!(find.path.prefix, Some("test-prefix/".to_string()));
    }

    #[test]
    fn test_find_to_stream() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);
        let page_size = 1000;

        let stream = FindStream {
            path: path.clone(),
            token: None,
            page_size,
            initial: true,
            client,
            maxdepth: None,
            all_versions: false,
        };

        assert_eq!(stream.path, path);
        assert_eq!(stream.token, None);
        assert_eq!(stream.page_size, page_size);
        assert!(stream.initial);
    }

    #[tokio::test]
    async fn test_find_from_opts() {
        let bucket = "test-bucket".to_string();
        let region = Region::new("test-region");
        let path = S3Path {
            bucket: bucket.clone(),
            prefix: Some("test-prefix/".to_string()),
        };

        let name_patterns = vec![Pattern::new("*.txt").unwrap()];
        let iname_globs = vec![InameGlob(Pattern::new("*.TXT").unwrap())];
        let regexes = vec![Regex::new(r"test.*\.txt").unwrap()];
        let sizes = vec![FindSize::Bigger(100)];
        let mtimes = vec![FindTime::Lower(3600 * 24)];

        let page_size = 500;
        let summarize = true;
        let limit = Some(100);

        let opts = FindOpt {
            aws_access_key: Some("test-access".to_string()),
            aws_secret_key: Some("test-secret".to_string()),
            aws_region: Some(region.clone()),
            endpoint_url: None,
            force_path_style: false,
            path: path.clone(),
            cmd: None,
            page_size,
            summarize,
            limit,
            name: name_patterns,
            iname: iname_globs,
            regex: regexes,
            size: sizes,
            mtime: mtimes,
            storage_class: Some(ObjectStorageClass::Standard),
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client = setup_client(&opts).await;

        let find = FindCommand::from_opts(&opts, client);

        assert_eq!(find.path.bucket, bucket);
    }

    #[tokio::test]
    async fn test_find_stream_list_with_replay_client() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let req_initial = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_initial_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>test-prefix/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>true</IsTruncated>
            <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
            <Contents>
                <Key>test-prefix/file1.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
                <Size>100</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
            <Contents>
                <Key>test-prefix/file2.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427f&quot;</ETag>
                <Size>200</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
        </ListBucketResult>"#;

        let resp_initial = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_initial_body))
            .unwrap();

        let req_continuation = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?continuation-token=1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D&list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_continuation_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>test-prefix/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>test-prefix/file3.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427g&quot;</ETag>
                <Size>300</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
        </ListBucketResult>"#;

        let resp_continuation = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_continuation_body))
            .unwrap();

        let events = vec![
            ReplayEvent::new(req_initial, resp_initial),
            ReplayEvent::new(req_continuation, resp_continuation),
        ];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let mut paginator = find_stream.paginator().await;
        let objects1 = paginator.next().await.unwrap().unwrap().contents.unwrap();

        assert_eq!(objects1.len(), 2, "First result should contain 2 objects");
        assert_eq!(objects1[0].key.as_ref().unwrap(), "test-prefix/file1.txt");
        assert_eq!(objects1[1].key.as_ref().unwrap(), "test-prefix/file2.txt");

        let objects2 = paginator.next().await.unwrap().unwrap().contents.unwrap();

        assert_eq!(objects2.len(), 1, "Second result should contain 1 object");
        assert_eq!(objects2[0].key.as_ref().unwrap(), "test-prefix/file3.txt");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_with_error_response() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let req_error = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_error = http::Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>AccessDenied</Code>
                    <Message>Access Denied</Message>
                    <RequestId>1D5H9EXAMPLE</RequestId>
                    <HostId>nh8QbPEXAMPLE</HostId>
                </Error>"#,
            ))
            .unwrap();

        let events = vec![ReplayEvent::new(req_error, resp_error)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let mut paginator = find_stream.paginator().await;
        let result = paginator.next().await;

        assert!(result.is_some(), "Expected Some result from paginator");
        let err = result.unwrap();
        assert!(err.is_err(), "Expected error response");

        if let Err(SdkError::ServiceError(service_error)) = err {
            assert_eq!(service_error.err().meta().code(), Some("AccessDenied"));
        }

        assert!(
            paginator.next().await.is_none(),
            "Expected None after error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_paginator_with_empty_page() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let mut paginator = find_stream.paginator().await;
        let result = paginator.next().await;

        assert!(result.is_some(), "Expected Some result from paginator");
        let output = result.unwrap().expect("Expected successful response");

        if let Some(contents) = output.contents {
            assert!(contents.is_empty(), "Expected empty contents list");
        }

        assert!(
            paginator.next().await.is_none(),
            "Expected None for next page"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_stream_with_empty_results() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <Contents></Contents>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let stream = find_stream.stream();

        let stream_objects: Vec<StreamObject> = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            stream_objects.len(),
            1,
            "Expected vector with 1 empty object"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_with_empty_page() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        };

        let stream = find_stream.stream();

        let stream_objects: Vec<StreamObject> = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(stream_objects.len(), 0, "Expected vector without object");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_from_opts() {
        let path1 = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let opts1 = FindOpt {
            aws_access_key: Some("test-access".to_string()),
            aws_secret_key: Some("test-secret".to_string()),
            aws_region: Some(Region::new("test-region")),
            endpoint_url: None,
            force_path_style: false,
            path: path1.clone(),
            cmd: None,
            page_size: 500,
            summarize: true,
            limit: Some(100),
            name: vec![],
            iname: vec![],
            regex: vec![],
            size: vec![],
            mtime: vec![],
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client1 = setup_client(&opts1).await;
        let find_stream1 = FindStream::from_opts(&opts1, client1);

        assert_eq!(find_stream1.path, path1);
        assert_eq!(find_stream1.page_size, 500);
        assert_eq!(find_stream1.token, None);
        assert!(find_stream1.initial);

        let path_without_prefix = S3Path {
            bucket: "another-bucket".to_string(),
            prefix: None,
        };

        let opts_withour_prefix = FindOpt {
            aws_access_key: None,
            aws_secret_key: None,
            aws_region: Some(Region::new("us-west-2")),
            endpoint_url: None,
            force_path_style: false,
            path: path_without_prefix.clone(),
            cmd: Some(Cmd::Ls(FastPrint {})),
            page_size: 1000,
            summarize: false,
            limit: None,
            name: vec![],
            iname: vec![],
            regex: vec![],
            size: vec![],
            mtime: vec![],
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client2 = setup_client(&opts_withour_prefix).await;
        let find_stream2 = FindStream::from_opts(&opts_withour_prefix, client2);

        assert_eq!(find_stream2.path, path_without_prefix);
        assert_eq!(find_stream2.page_size, 1000);
        assert_eq!(find_stream2.token, None);
        assert!(find_stream2.initial);
    }

    #[tokio::test]
    async fn test_maxdepth_zero() -> Result<(), Box<dyn std::error::Error>> {
        // Test that maxdepth=0 returns only objects at prefix level
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("data/".to_string()),
        };

        let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=1000&prefix=data%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <Delimiter>/</Delimiter>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>data/file1.txt</Key>
                <Size>100</Size>
            </Contents>
            <Contents>
                <Key>data/file2.txt</Key>
                <Size>200</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>data/2024/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: Some(0),
            all_versions: false,
        };

        let stream = find_stream.stream();
        let stream_objects: Vec<StreamObject> = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        // With maxdepth=0, should only get objects at prefix level
        assert_eq!(stream_objects.len(), 2);
        assert_eq!(stream_objects[0].key().unwrap(), "data/file1.txt");
        assert_eq!(stream_objects[1].key().unwrap(), "data/file2.txt");

        Ok(())
    }

    #[tokio::test]
    async fn test_maxdepth_one_with_delimiter() -> Result<(), Box<dyn std::error::Error>> {
        // Test that maxdepth=1 uses delimiter and recurses one level
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("logs/".to_string()),
        };

        // First request: root level with delimiter
        let req1 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=logs%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp1_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>logs/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>logs/root.txt</Key>
                <Size>100</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>logs/2024/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp1_body))
            .unwrap();

        // Second request: subdirectory level with delimiter
        let req2 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=logs%2F2024%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp2_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>logs/2024/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>logs/2024/jan.txt</Key>
                <Size>200</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>logs/2024/01/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp2_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 100,
            initial: true,
            maxdepth: Some(1),
            all_versions: false,
        };

        let stream = find_stream.stream();
        let objects = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        // Should get objects from root level and one subdirectory level
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key().unwrap(), "logs/root.txt");
        assert_eq!(objects[1].key().unwrap(), "logs/2024/jan.txt");
        // Should NOT recurse into logs/2024/01/ due to maxdepth=1

        Ok(())
    }

    #[tokio::test]
    async fn test_maxdepth_none_uses_standard_pagination() {
        // Test that maxdepth=None uses standard pagination without delimiter
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("data/".to_string()),
        };

        let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=data%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>data/file.txt</Key>
                <Size>100</Size>
            </Contents>
        </ListBucketResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None, // No maxdepth - should use standard pagination
            all_versions: false,
        };

        let stream = find_stream.stream();
        let objects = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key().unwrap(), "data/file.txt");
    }

    #[tokio::test]
    async fn test_maxdepth_with_empty_subdirectories() -> Result<(), Box<dyn std::error::Error>> {
        // Test that empty subdirectories don't cause issues
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty/".to_string()),
        };

        // Root level
        let req1 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=empty%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp1_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>empty/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <CommonPrefixes>
                <Prefix>empty/sub/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp1_body))
            .unwrap();

        // Empty subdirectory
        let req2 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=empty%2Fsub%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp2_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>empty/sub/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>"#;

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp2_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 100,
            initial: true,
            maxdepth: Some(1),
            all_versions: false,
        };

        let stream = find_stream.stream();
        let objects = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        // Empty subdirectory should yield no objects but not cause errors
        assert_eq!(objects.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_all_versions_listing() -> Result<(), Box<dyn std::error::Error>> {
        // Test that all_versions uses ListObjectVersions API
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("data/".to_string()),
        };

        let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?max-keys=1000&prefix=data%2F&versions=")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Version>
                <Key>data/file1.txt</Key>
                <VersionId>v1</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <Size>100</Size>
                <StorageClass>STANDARD</StorageClass>
            </Version>
            <Version>
                <Key>data/file1.txt</Key>
                <VersionId>v0</VersionId>
                <IsLatest>false</IsLatest>
                <LastModified>2022-12-01T00:00:00.000Z</LastModified>
                <Size>90</Size>
                <StorageClass>STANDARD</StorageClass>
            </Version>
            <DeleteMarker>
                <Key>data/deleted.txt</Key>
                <VersionId>dm1</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2023-02-01T00:00:00.000Z</LastModified>
            </DeleteMarker>
        </ListVersionsResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: true, // Enable version listing
        };

        let stream = find_stream.stream();
        let objects = stream
            .flat_map(|x| futures::stream::iter(x.into_iter()))
            .collect::<Vec<_>>()
            .await;

        // Should get all versions and delete markers
        assert_eq!(objects.len(), 3);

        // Objects are sorted by key (alphabetically), then by last_modified descending
        // So deleted.txt comes before file1.txt

        // Check delete marker (first alphabetically: "data/deleted.txt" < "data/file1.txt")
        assert_eq!(objects[0].key().unwrap(), "data/deleted.txt");
        assert_eq!(objects[0].version_id.as_ref().unwrap(), "dm1");
        assert_eq!(objects[0].is_latest, Some(true));
        assert!(objects[0].is_delete_marker);
        assert_eq!(
            objects[0].display_key(),
            "data/deleted.txt?versionId=dm1 (latest) (delete marker)"
        );
        // Delete marker has size 0
        assert_eq!(objects[0].object.size(), Some(0));

        // Check first version of file1.txt (latest, newer last_modified)
        assert_eq!(objects[1].key().unwrap(), "data/file1.txt");
        assert_eq!(objects[1].version_id.as_ref().unwrap(), "v1");
        assert_eq!(objects[1].is_latest, Some(true));
        assert_eq!(
            objects[1].display_key(),
            "data/file1.txt?versionId=v1 (latest)"
        );

        // Check older version of file1.txt
        assert_eq!(objects[2].key().unwrap(), "data/file1.txt");
        assert_eq!(objects[2].version_id.as_ref().unwrap(), "v0");
        assert_eq!(objects[2].is_latest, Some(false));
        assert_eq!(objects[2].display_key(), "data/file1.txt?versionId=v0");

        Ok(())
    }

    #[test]
    fn test_stream_object_from_version_with_version_id() {
        use aws_sdk_s3::types::ObjectVersion;
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
            stream_obj.display_key(),
            "test.txt?versionId=abc123 (latest)"
        );
        assert_eq!(stream_obj.object.size(), Some(100));
    }

    #[test]
    fn test_stream_object_from_version_not_latest() {
        use aws_sdk_s3::types::ObjectVersion;

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
        assert_eq!(stream_obj.display_key(), "test.txt?versionId=old123");
        assert_eq!(stream_obj.object.size(), Some(50));
    }

    #[test]
    fn test_stream_object_from_delete_marker() {
        use aws_sdk_s3::types::DeleteMarkerEntry;

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
            stream_obj.display_key(),
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
        assert_eq!(stream_obj.display_key(), "simple.txt");
        assert_eq!(stream_obj.object.size(), Some(200));
    }
}
