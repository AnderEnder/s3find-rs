use std::fmt;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_smithy_async::future::pagination_stream::PaginationStream;
use futures::StreamExt;

use crate::arg::{FindOpt, S3Path};
use crate::error::{S3FindError, S3FindResult};

use super::{BoxedStream, S3_PATH_DELIMITER, S3ListResult, StreamObject};

pub struct FindStream {
    pub client: Client,
    pub path: S3Path,
    pub token: Option<String>,
    pub page_size: u16,
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

    pub(crate) async fn paginator(self) -> PaginationStream<S3ListResult<ListObjectsV2Output>> {
        self.client
            .list_objects_v2()
            .bucket(self.path.bucket.clone())
            .prefix(self.path.prefix.clone().unwrap_or_else(|| "".to_owned()))
            .max_keys(i32::from(self.page_size))
            .into_paginator()
            .send()
    }

    fn collect_objects_recursive<'a>(
        client: &'a Client,
        bucket: &'a str,
        prefix: String,
        maxdepth: usize,
        current_depth: usize,
        page_size: i32,
    ) -> BoxedStream<'a, S3FindResult<StreamObject>> {
        Box::pin(async_stream::stream! {
            if current_depth > maxdepth {
                return;
            }

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
                        if let Some(contents) = output.contents {
                            for obj in contents {
                                yield Ok(StreamObject::from_object(obj));
                            }
                        }

                        if current_depth < maxdepth
                            && let Some(common_prefixes) = output.common_prefixes
                        {
                            for common_prefix in common_prefixes {
                                if let Some(prefix_str) = common_prefix.prefix {
                                    let mut sub_stream = Self::collect_objects_recursive(
                                        client,
                                        bucket,
                                        prefix_str,
                                        maxdepth,
                                        current_depth + 1,
                                        page_size,
                                    );

                                    while let Some(item) = sub_stream.next().await {
                                        yield item;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(S3FindError::list_objects(e));
                        break;
                    }
                }
            }
        })
    }

    fn paginator_with_depth(self) -> BoxedStream<'static, S3FindResult<Vec<StreamObject>>> {
        let maxdepth = self.maxdepth.unwrap_or(usize::MAX);
        let base_prefix = self.path.prefix.clone().unwrap_or_else(|| "".to_owned());
        let bucket = self.path.bucket.clone();
        let page_size = self.page_size;
        let page_size_i32 = i32::from(page_size);
        let page_size_usize = usize::from(page_size);

        Box::pin(async_stream::stream! {
            let obj_stream = Self::collect_objects_recursive(
                &self.client,
                &bucket,
                base_prefix,
                maxdepth,
                0,
                page_size_i32,
            );

            futures::pin_mut!(obj_stream);

            let mut chunk = Vec::with_capacity(page_size_usize);

            while let Some(result) = obj_stream.next().await {
                match result {
                    Ok(obj) => {
                        chunk.push(obj);
                        if chunk.len() >= page_size_usize {
                            yield Ok(std::mem::take(&mut chunk));
                            chunk = Vec::with_capacity(page_size_usize);
                        }
                    }
                    Err(err) => {
                        yield Err(err);
                        break;
                    }
                }
            }

            if !chunk.is_empty() {
                yield Ok(chunk);
            }
        })
    }

    fn versions_paginator(self) -> BoxedStream<'static, S3FindResult<Vec<StreamObject>>> {
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
                    .max_keys(i32::from(page_size));

                if let Some(ref km) = key_marker {
                    request = request.key_marker(km);
                }
                if let Some(ref vim) = version_id_marker {
                    request = request.version_id_marker(vim);
                }

                match request.send().await {
                    Ok(output) => {
                        let mut stream_objects = Vec::new();

                        if let Some(versions) = output.versions {
                            for version in versions {
                                stream_objects.push(StreamObject::from_version(version));
                            }
                        }

                        if let Some(markers) = output.delete_markers {
                            for marker in markers {
                                stream_objects.push(StreamObject::from_delete_marker(marker));
                            }
                        }

                        stream_objects.sort_by(|a, b| {
                            let key_cmp = a.object.key().cmp(&b.object.key());
                            if key_cmp != std::cmp::Ordering::Equal {
                                return key_cmp;
                            }
                            b.object.last_modified().cmp(&a.object.last_modified())
                        });

                        if !stream_objects.is_empty() {
                            yield Ok(stream_objects);
                        }

                        if output.is_truncated.unwrap_or(false) {
                            key_marker = output.next_key_marker;
                            version_id_marker = output.next_version_id_marker;
                        } else {
                            break;
                        }
                    }
                    Err(e) => {
                        yield Err(S3FindError::list_object_versions(e));
                        break;
                    }
                }
            }
        })
    }

    pub fn stream(self) -> BoxedStream<'static, S3FindResult<Vec<StreamObject>>> {
        if self.all_versions {
            if self.maxdepth.is_some() {
                eprintln!("Warning: --maxdepth is ignored when --all-versions is used");
            }
            return self.versions_paginator();
        }

        if self.maxdepth.is_some() {
            return self.paginator_with_depth();
        }

        Box::pin(async_stream::stream! {
            let mut ps = self.paginator().await;

            while let Some(result) = ps.next().await {
                match result {
                    Ok(output) => {
                        let objects = output.contents.unwrap_or_default();
                        if !objects.is_empty() {
                            let stream_objects: Vec<StreamObject> = objects
                                .into_iter()
                                .map(StreamObject::from_object)
                                .collect();
                            yield Ok(stream_objects);
                        }
                    }
                    Err(e) => {
                        yield Err(S3FindError::list_objects(e));
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
