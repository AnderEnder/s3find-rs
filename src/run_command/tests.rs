use super::*;
use crate::adapters::aws::{
    CommandS3Client, DeleteObjectRequest, DeletedObjectInfo, RestoreObjectStatus,
};
use anyhow::anyhow;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    primitives::{ByteStream, DateTime},
    types::{MetadataDirective, Object, ObjectStorageClass, StorageClass, Tag, Tier},
};
use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::date_time::Format;
use http::{HeaderValue, StatusCode};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tempfile::tempdir;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectRequestKey {
    bucket: String,
    key: String,
    version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PutTagCall {
    object: ObjectRequestKey,
    tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyCall {
    bucket: String,
    key: String,
    copy_source: String,
    storage_class: Option<String>,
    metadata_directive: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestoreCall {
    object: ObjectRequestKey,
    days: i32,
    tier: String,
}

struct FakeCommandClient {
    region: Option<String>,
    delete_calls: Mutex<Vec<(String, Vec<DeleteObjectRequest>)>>,
    put_tag_calls: Mutex<Vec<PutTagCall>>,
    get_tag_calls: Mutex<Vec<ObjectRequestKey>>,
    public_calls: Mutex<Vec<ObjectRequestKey>>,
    get_object_calls: Mutex<Vec<ObjectRequestKey>>,
    copy_calls: Mutex<Vec<CopyCall>>,
    restore_calls: Mutex<Vec<RestoreCall>>,
    object_bodies: Mutex<HashMap<ObjectRequestKey, Vec<u8>>>,
    tag_outputs: Mutex<HashMap<ObjectRequestKey, Vec<Tag>>>,
    restore_outputs: Mutex<HashMap<ObjectRequestKey, RestoreObjectStatus>>,
}

impl Default for FakeCommandClient {
    fn default() -> Self {
        Self {
            region: Some("us-east-1".to_string()),
            delete_calls: Mutex::new(Vec::new()),
            put_tag_calls: Mutex::new(Vec::new()),
            get_tag_calls: Mutex::new(Vec::new()),
            public_calls: Mutex::new(Vec::new()),
            get_object_calls: Mutex::new(Vec::new()),
            copy_calls: Mutex::new(Vec::new()),
            restore_calls: Mutex::new(Vec::new()),
            object_bodies: Mutex::new(HashMap::new()),
            tag_outputs: Mutex::new(HashMap::new()),
            restore_outputs: Mutex::new(HashMap::new()),
        }
    }
}

impl FakeCommandClient {
    fn object_key(bucket: &str, key: &str, version_id: Option<&str>) -> ObjectRequestKey {
        ObjectRequestKey {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(str::to_string),
        }
    }

    fn with_object_body(
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

    fn with_tags(self, bucket: &str, key: &str, version_id: Option<&str>, tags: Vec<Tag>) -> Self {
        self.tag_outputs
            .lock()
            .unwrap()
            .insert(Self::object_key(bucket, key, version_id), tags);
        self
    }

    fn with_restore_status(
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

#[async_trait::async_trait]
impl CommandS3Client for FakeCommandClient {
    fn region(&self) -> Option<String> {
        self.region.clone()
    }

    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> Result<Vec<DeletedObjectInfo>, anyhow::Error> {
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
    ) -> Result<(), anyhow::Error> {
        self.put_tag_calls.lock().unwrap().push(PutTagCall {
            object: FakeCommandClient::object_key(bucket, key, version_id),
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
    ) -> Result<Vec<Tag>, anyhow::Error> {
        let object = FakeCommandClient::object_key(bucket, key, version_id);
        self.get_tag_calls.lock().unwrap().push(object.clone());
        Ok(self
            .tag_outputs
            .lock()
            .unwrap()
            .get(&object)
            .cloned()
            .unwrap_or_default())
    }

    async fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        self.public_calls
            .lock()
            .unwrap()
            .push(FakeCommandClient::object_key(bucket, key, version_id));
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ByteStream, anyhow::Error> {
        let object = FakeCommandClient::object_key(bucket, key, version_id);
        self.get_object_calls.lock().unwrap().push(object.clone());
        let body = self
            .object_bodies
            .lock()
            .unwrap()
            .get(&object)
            .cloned()
            .ok_or_else(|| anyhow!("missing object body for {}", key))?;
        Ok(ByteStream::from(body))
    }

    async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> Result<(), anyhow::Error> {
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
    ) -> Result<RestoreObjectStatus, anyhow::Error> {
        let object = FakeCommandClient::object_key(bucket, key, version_id);
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

#[test]
fn test_advanced_print_object() -> Result<(), Error> {
    let mut buf = Vec::new();
    let cmd = AdvancedPrint::default();
    let bucket = "test";

    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
    let out = std::str::from_utf8(&buf)?;

    println!("{}", out);
    assert!(out.contains("9d48114aa7c18f9d68aa20086dbb7756"));
    assert!(out.contains("None"));
    assert!(out.contains("4997288"));
    assert!(out.contains("2017-07-19T19:04:17Z"));
    assert!(out.contains("s3://test/somepath/otherpath"));
    assert!(out.contains("STANDARD"));
    Ok(())
}

#[tokio::test]
async fn test_multiple_delete_with_fake_client_records_version_ids() -> Result<(), Error> {
    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };

    let objects = vec![
        StreamObject {
            object: Object::builder().key("test/file1.txt").build(),
            version_id: Some("v1".to_string()),
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        },
        StreamObject::from_object(Object::builder().key("test/file2.txt").build()),
    ];

    Cmd::Delete(MultipleDelete {})
        .downcast()
        .execute(&client, &path, &objects)
        .await?;

    let delete_calls = client.delete_calls.lock().unwrap();
    assert_eq!(delete_calls.len(), 1);
    assert_eq!(delete_calls[0].0, "test-bucket");
    assert_eq!(
        delete_calls[0].1,
        vec![
            DeleteObjectRequest {
                key: "test/file1.txt".to_string(),
                version_id: Some("v1".to_string()),
            },
            DeleteObjectRequest {
                key: "test/file2.txt".to_string(),
                version_id: None,
            },
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_set_tags_with_fake_client_records_tags() -> Result<(), Error> {
    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject::from_object(
        Object::builder().key("test/file1.txt").build(),
    )];

    Cmd::Tags(SetTags {
        tags: vec![
            FindTag {
                key: "env".to_string(),
                value: "prod".to_string(),
            },
            FindTag {
                key: "team".to_string(),
                value: "data".to_string(),
            },
        ],
    })
    .downcast()
    .execute(&client, &path, &objects)
    .await?;

    let put_tag_calls = client.put_tag_calls.lock().unwrap();
    assert_eq!(put_tag_calls.len(), 1);
    assert_eq!(put_tag_calls[0].object.key, "test/file1.txt");
    assert_eq!(
        put_tag_calls[0].tags,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tags_with_fake_client_fetches_tags() -> Result<(), Error> {
    let client = FakeCommandClient::default().with_tags(
        "test-bucket",
        "test/file1.txt",
        Some("v1"),
        vec![Tag::builder().key("env").value("prod").build().unwrap()],
    );
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject {
        object: Object::builder().key("test/file1.txt").build(),
        version_id: Some("v1".to_string()),
        is_latest: None,
        is_delete_marker: false,
        tags: None,
    }];

    Cmd::LsTags(ListTags {})
        .downcast()
        .execute(&client, &path, &objects)
        .await?;

    let get_tag_calls = client.get_tag_calls.lock().unwrap();
    assert_eq!(get_tag_calls.len(), 1);
    assert_eq!(get_tag_calls[0].key, "test/file1.txt");
    assert_eq!(get_tag_calls[0].version_id.as_deref(), Some("v1"));

    Ok(())
}

#[tokio::test]
async fn test_download_with_fake_client_writes_file() -> Result<(), Error> {
    let temp_dir = tempdir()?;
    let client = FakeCommandClient::default().with_object_body(
        "test-bucket",
        "test/file.txt",
        Some("v1"),
        b"hello world",
    );
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject {
        object: Object::builder().key("test/file.txt").size(11).build(),
        version_id: Some("v1".to_string()),
        is_latest: None,
        is_delete_marker: false,
        tags: None,
    }];

    Cmd::Download(Download {
        destination: temp_dir.path().to_string_lossy().to_string(),
        force: true,
    })
    .downcast()
    .execute(&client, &path, &objects)
    .await?;

    let file_path = temp_dir.path().join("test/file.txt.vv1");
    assert_eq!(fs::read_to_string(file_path)?, "hello world");
    assert_eq!(client.get_object_calls.lock().unwrap().len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_s3_copy_with_fake_client_records_request() -> Result<(), Error> {
    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "source-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject {
        object: Object::builder().key("source/file.txt").build(),
        version_id: Some("v1".to_string()),
        is_latest: None,
        is_delete_marker: false,
        tags: None,
    }];

    Cmd::Copy(S3Copy {
        destination: S3Path {
            bucket: "dest-bucket".to_string(),
            prefix: Some("archive".to_string()),
        },
        flat: false,
        storage_class: Some(StorageClass::Glacier),
    })
    .downcast()
    .execute(&client, &path, &objects)
    .await?;

    let copy_calls = client.copy_calls.lock().unwrap();
    assert_eq!(copy_calls.len(), 1);
    assert_eq!(copy_calls[0].bucket, "dest-bucket");
    assert_eq!(copy_calls[0].key, "archive/source/file.txt");
    assert_eq!(
        copy_calls[0].copy_source,
        "source-bucket/source%2Ffile.txt?versionId=v1"
    );
    assert_eq!(copy_calls[0].storage_class.as_deref(), Some("GLACIER"));
    assert!(copy_calls[0].metadata_directive.is_none());

    Ok(())
}

#[tokio::test]
async fn test_s3_move_with_fake_client_records_copy_and_delete() -> Result<(), Error> {
    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "source-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject::from_object(
        Object::builder().key("source/file.txt").build(),
    )];

    Cmd::Move(S3Move {
        destination: S3Path {
            bucket: "dest-bucket".to_string(),
            prefix: Some("archive".to_string()),
        },
        flat: false,
        storage_class: Some(StorageClass::StandardIa),
    })
    .downcast()
    .execute(&client, &path, &objects)
    .await?;

    let copy_calls = client.copy_calls.lock().unwrap();
    assert_eq!(copy_calls.len(), 1);
    assert_eq!(copy_calls[0].metadata_directive.as_deref(), Some("COPY"));

    let delete_calls = client.delete_calls.lock().unwrap();
    assert_eq!(delete_calls.len(), 1);
    assert_eq!(delete_calls[0].1[0].key, "source/file.txt");

    Ok(())
}

#[tokio::test]
async fn test_restore_with_fake_client_records_request() -> Result<(), Error> {
    let client = FakeCommandClient::default().with_restore_status(
        "test-bucket",
        "archive/file.txt",
        Some("v1"),
        RestoreObjectStatus::AlreadyInProgress,
    );
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject {
        object: Object::builder()
            .key("archive/file.txt")
            .storage_class(ObjectStorageClass::Glacier)
            .build(),
        version_id: Some("v1".to_string()),
        is_latest: None,
        is_delete_marker: false,
        tags: None,
    }];

    Cmd::Restore(Restore {
        days: 7,
        tier: Tier::Bulk,
    })
    .downcast()
    .execute(&client, &path, &objects)
    .await?;

    let restore_calls = client.restore_calls.lock().unwrap();
    assert_eq!(restore_calls.len(), 1);
    assert_eq!(restore_calls[0].object.key, "archive/file.txt");
    assert_eq!(restore_calls[0].days, 7);
    assert_eq!(restore_calls[0].tier, "Bulk");

    Ok(())
}

#[test]
fn test_advanced_print_object_with_owner_no_date_no_storage() -> Result<(), Error> {
    let mut buf = Vec::new();
    let cmd = AdvancedPrint::default();
    let bucket = "test";

    let owner = aws_sdk_s3::types::Owner::builder()
        .display_name("test-owner")
        .id("owner-id-123456789")
        .build();

    let object = Object::builder()
        .e_tag("abc123456789")
        .key("some/test/file.txt")
        .size(1_234_567)
        .set_owner(Some(owner))
        .build();
    let stream_obj = StreamObject::from_object(object);

    cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
    let out = std::str::from_utf8(&buf)?;

    println!("Output with owner, no date, no storage: {}", out);
    assert!(out.contains("abc123456789"));
    assert!(out.contains("test-owner"));
    assert!(out.contains("1234567"));
    assert!(out.contains("None"));
    assert!(out.contains("s3://test/some/test/file.txt"));
    assert!(out.contains("None"));
    Ok(())
}

#[test]
fn test_fast_print_object() -> Result<(), Error> {
    let mut buf = Vec::new();
    let cmd = FastPrint {};
    let bucket = "test";

    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
    let out = std::str::from_utf8(&buf)?;

    assert!(out.contains("s3://test/somepath/otherpath"));
    Ok(())
}

#[test]
fn test_exec() -> Result<(), Error> {
    let mut buf = Vec::new();
    let cmd = Exec {
        utility: "echo test {}".to_owned(),
    };

    let path = "s3://test/somepath/otherpath";
    cmd.exec(&mut buf, path)?;
    let out = std::str::from_utf8(&buf)?;

    assert!(out.contains("test"));
    assert!(out.contains("s3://test/somepath/otherpath"));
    Ok(())
}

#[test]
fn test_advanced_print_json() -> Result<(), Error> {
    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, &stream_obj)?;

    let output = String::from_utf8(buf).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        parsed["e_tag"].as_str().unwrap(),
        "9d48114aa7c18f9d68aa20086dbb7756"
    );
    assert_eq!(parsed["key"].as_str().unwrap(), "somepath/otherpath");
    assert_eq!(parsed["size"].as_i64().unwrap(), 4_997_288);
    assert_eq!(parsed["storage_class"].as_str().unwrap(), "STANDARD");
    assert_eq!(
        parsed["last_modified"].as_str().unwrap(),
        "2017-07-19T19:04:17Z"
    );
    assert_eq!(parsed["owner"].as_str().unwrap(), "");

    Ok(())
}

#[test]
fn test_advanced_print_csv() -> Result<(), Error> {
    let object1 = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("abcdef1234567890")
        .key("another/path")
        .size(1024)
        .storage_class(ObjectStorageClass::ReducedRedundancy)
        .last_modified(DateTime::from_str(
            "2020-01-01T12:30:45.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Csv,
    };

    cmd.print_csv_stream_objects(&mut buf, &[stream_obj1.clone(), stream_obj2.clone()])?;

    let output = String::from_utf8(buf.clone()).unwrap();
    println!("CSV Output:\n{}", output);

    let record1: ObjectRecord = (&stream_obj1).into();
    let record2: ObjectRecord = (&stream_obj2).into();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(&*buf);

    let records: Vec<ObjectRecord> = reader
        .deserialize()
        .collect::<Result<Vec<ObjectRecord>, _>>()?;

    assert_eq!(records.len(), 2, "Should have exactly two records");

    assert_eq!(records[0].e_tag, record1.e_tag);
    assert_eq!(records[0].key, record1.key);
    assert_eq!(records[0].size, record1.size);
    assert_eq!(records[0].storage_class, record1.storage_class);
    assert_eq!(records[0].last_modified, record1.last_modified);

    assert_eq!(records[1].e_tag, record2.e_tag);
    assert_eq!(records[1].key, record2.key);
    assert_eq!(records[1].size, record2.size);
    assert_eq!(records[1].storage_class, record2.storage_class);
    assert_eq!(records[1].last_modified, record2.last_modified);

    Ok(())
}

#[tokio::test]
async fn test_advanced_print() -> Result<(), Error> {
    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Print(AdvancedPrint::default()).downcast();
    let client = FakeCommandClient::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;
    Ok(())
}

#[tokio::test]
async fn smoke_advanced_print_json_execute() -> Result<(), Error> {
    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key("folder/file1.txt")
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key("folder/file2.txt")
        .size(200)
        .storage_class(ObjectStorageClass::IntelligentTiering)
        .last_modified(DateTime::from_str("2023-02-15T12:30:45.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let cmd = Cmd::Print(AdvancedPrint {
        format: PrintFormat::Json,
    })
    .downcast();

    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;
    Ok(())
}

#[tokio::test]
async fn smoke_advanced_print_csv_execute() -> Result<(), Error> {
    let object1 = Object::builder()
        .e_tag("csv-etag-1")
        .key("data/report.csv")
        .size(5000)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("csv-etag-2")
        .key("data/archive.zip")
        .size(10000)
        .storage_class(ObjectStorageClass::DeepArchive)
        .last_modified(DateTime::from_str("2023-04-20T14:25:10.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let cmd = Cmd::Print(AdvancedPrint {
        format: PrintFormat::Csv,
    })
    .downcast();

    let client = FakeCommandClient::default();
    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_fastprint() -> Result<(), Error> {
    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Ls(FastPrint {}).downcast();
    let client = FakeCommandClient::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;
    Ok(())
}

#[tokio::test]
async fn smoke_donothing() -> Result<(), Error> {
    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Nothing(DoNothing {}).downcast();
    let client = FakeCommandClient::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await
}

#[tokio::test]
async fn smoke_exec() -> Result<(), Error> {
    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            Format::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Exec(Exec {
        utility: "echo {}".to_owned(),
    })
    .downcast();

    let client = FakeCommandClient::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await
}

#[test]
fn test_generate_s3_url() {
    assert_eq!(
        &generate_s3_url("us-east-1", "test-bucket", "somepath/somekey"),
        "https://test-bucket.s3.amazonaws.com/somepath/somekey",
    );
    assert_eq!(
        &generate_s3_url("eu-west-1", "test-bucket", "somepath/somekey"),
        "https://test-bucket.s3-eu-west-1.amazonaws.com/somepath/somekey",
    );
    assert_eq!(
        &generate_s3_url("us-east-1", "test-bucket", "some path/file name.txt"),
        "https://test-bucket.s3.amazonaws.com/some%20path/file%20name.txt",
    );
}

fn make_s3_test_credentials() -> aws_sdk_s3::config::Credentials {
    aws_sdk_s3::config::Credentials::new(
        "ATESTCLIENT",
        "astestsecretkey",
        Some("atestsessiontoken".to_string()),
        None,
        "",
    )
}

#[tokio::test]
async fn test_download_with_replay_client() -> Result<(), Error> {
    let temp_dir = tempdir()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let key = "test/file.txt";
    let object = Object::builder()
        .e_tag("test-etag")
        .key(key)
        .size(14) // Size of "Test content\n"
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj = StreamObject::from_object(object);

    let content = "Test content\n";

    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/test/file.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("text/plain"))
        .header("ETag", HeaderValue::from_static("\"test-etag\""))
        .header("Content-Length", HeaderValue::from_static("13"))
        .body(SdkBody::from(content))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::Download(Download {
        destination: temp_path.clone(),
        force: true,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;

    let file_path = PathBuf::from(temp_path).join(key);
    assert!(file_path.exists(), "Downloaded file should exist");

    let downloaded_content = fs::read_to_string(&file_path)?;
    assert_eq!(downloaded_content, "Test content\n");

    Ok(())
}

#[tokio::test]
async fn test_download_skips_existing_file_and_continues() -> Result<(), Error> {
    let temp_dir = tempdir()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let existing_key = "test/existing.txt";
    let new_key = "test/new.txt";

    let existing_path = PathBuf::from(&temp_path).join(existing_key);
    std::fs::create_dir_all(existing_path.parent().unwrap())?;
    std::fs::write(&existing_path, "keep me")?;

    let stream_obj_existing =
        StreamObject::from_object(Object::builder().key(existing_key).size(7).build());
    let stream_obj_new = StreamObject::from_object(Object::builder().key(new_key).size(12).build());

    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/test/new.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("text/plain"))
        .header("Content-Length", HeaderValue::from_static("12"))
        .body(SdkBody::from("new content\n"))
        .unwrap();

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(StaticReplayClient::new(vec![ReplayEvent::new(req, resp)]))
            .build(),
    );

    let cmd = Cmd::Download(Download {
        destination: temp_path.clone(),
        force: false,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj_existing, stream_obj_new])
        .await?;

    assert_eq!(std::fs::read_to_string(existing_path)?, "keep me");
    assert_eq!(
        std::fs::read_to_string(PathBuf::from(temp_path).join(new_key))?,
        "new content\n"
    );

    Ok(())
}

#[tokio::test]
async fn test_set_public_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?acl")
        .body(SdkBody::empty())
        .unwrap();

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?acl")
        .body(SdkBody::empty())
        .unwrap();

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::Public(SetPublic {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_delete_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/?delete")
        .body(SdkBody::empty()) // In a real request this would contain XML with keys to delete
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file1.txt</Key>
            </Deleted>
            <Deleted>
                <Key>test/file2.txt</Key>
            </Deleted>
        </DeleteResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::Delete(MultipleDelete {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_delete_propagates_request_failure() {
    let object = Object::builder().key("test/file1.txt").size(100).build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/?delete")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>AccessDenied</Code>
                    <Message>Access Denied</Message>
                </Error>"#,
        ))
        .unwrap();

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(StaticReplayClient::new(vec![ReplayEvent::new(req, resp)]))
            .build(),
    );

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = Cmd::Delete(MultipleDelete {})
        .downcast()
        .execute(&client, &path, &[StreamObject::from_object(object)])
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_set_tags_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?tagging")
        .body(SdkBody::empty()) // In a real request this would contain XML with tags
        .unwrap();

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?tagging")
        .body(SdkBody::empty())
        .unwrap();

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let tags = vec![
        FindTag {
            key: "tag1".to_owned(),
            value: "value1".to_owned(),
        },
        FindTag {
            key: "tag2".to_owned(),
            value: "value2".to_owned(),
        },
    ];

    let cmd = Cmd::Tags(SetTags { tags }).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_list_tags_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req1 = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?tagging")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>key1</Key>
                    <Value>value1</Value>
                </Tag>
                <Tag>
                    <Key>key2</Key>
                    <Value>value2</Value>
                </Tag>
            </TagSet>
        </Tagging>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body1))
        .unwrap();

    let req2 = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?tagging")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>category</Key>
                    <Value>documents</Value>
                </Tag>
            </TagSet>
        </Tagging>"#;

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body2))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::LsTags(ListTags {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_list_tags_with_version_id() -> Result<(), Error> {
    // Test ListTags with version_id (versioned object)
    let key1 = "test/versioned.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    // Request includes versionId query parameter
    let req1 = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/test/versioned.txt?tagging&versionId=v123")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>version</Key>
                    <Value>v123</Value>
                </Tag>
            </TagSet>
        </Tagging>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body1))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::LsTags(ListTags {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    // Create versioned StreamObject
    let stream_obj = StreamObject {
        object: object1,
        version_id: Some("v123".to_string()),
        is_latest: Some(true),
        is_delete_marker: false,
        tags: None, // Force API fetch
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_list_tags_with_cached_tags() -> Result<(), Error> {
    // Test that ListTags uses cached tags when available (no API calls needed)
    use aws_sdk_s3::types::Tag;

    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    // No HTTP events needed - tags are cached
    let replay_client = StaticReplayClient::new(vec![]);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::LsTags(ListTags {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    // Create StreamObjects with pre-cached tags
    let mut stream_obj1 = StreamObject::from_object(object1);
    stream_obj1.tags = Some(vec![
        Tag::builder().key("env").value("prod").build().unwrap(),
        Tag::builder().key("team").value("data").build().unwrap(),
    ]);

    let mut stream_obj2 = StreamObject::from_object(object2);
    stream_obj2.tags = Some(vec![
        Tag::builder()
            .key("category")
            .value("documents")
            .build()
            .unwrap(),
    ]);

    // Execute - should use cached tags without making API calls
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_list_tags_skips_delete_markers() -> Result<(), Error> {
    // Test that ListTags skips delete markers (they can't have tags)
    let key1 = "test/file1.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    // No HTTP events needed - delete marker should be skipped
    let replay_client = StaticReplayClient::new(vec![]);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::LsTags(ListTags {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    // Create a delete marker StreamObject
    let stream_obj = StreamObject {
        object: object1,
        version_id: Some("delete-marker-version".to_string()),
        is_latest: Some(true),
        is_delete_marker: true, // This is a delete marker
        tags: None,
    };

    // Execute - should skip the delete marker without making API calls
    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_copy_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/dest/file1.txt")
        .header("x-amz-copy-source", "test-bucket/test/file1.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body1))
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/dest/file2.txt")
        .header("x-amz-copy-source", "test-bucket/test/file2.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body2))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: None,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_copy_with_storage_class() -> Result<(), Error> {
    let key = "test/file_to_copy.txt";

    let object = Object::builder()
        .e_tag("test-etag")
        .key(key)
        .size(1000)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/archive/file_to_copy.txt")
        .header("x-amz-copy-source", "test-bucket/test/file_to_copy.txt")
        .header("x-amz-storage-class", "GLACIER")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("archive/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: Some(StorageClass::Glacier),
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj = StreamObject::from_object(object);
    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_move_with_storage_class() -> Result<(), Error> {
    let key = "test/file_to_move.txt";

    let object = Object::builder()
        .e_tag("test-etag")
        .key(key)
        .size(1000)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req_copy = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/archive/file_to_move.txt")
        .header("x-amz-copy-source", "test-bucket/test/file_to_move.txt")
        .header("x-amz-storage-class", "INTELLIGENT_TIERING")
        .body(SdkBody::empty())
        .unwrap();

    let resp_copy_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

    let resp_copy = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_copy_body))
        .unwrap();

    let req_delete = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/?delete")
        .body(SdkBody::empty())
        .unwrap();

    let resp_delete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file_to_move.txt</Key>
            </Deleted>
        </DeleteResult>"#;

    let resp_delete = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_delete_body))
        .unwrap();

    let events = vec![
        ReplayEvent::new(req_copy, resp_copy),
        ReplayEvent::new(req_delete, resp_delete),
    ];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("archive/".to_owned()),
    };

    let cmd = Cmd::Move(S3Move {
        destination,
        flat: true,
        storage_class: Some(StorageClass::IntelligentTiering),
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj = StreamObject::from_object(object);
    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_move_with_replay_client() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req_copy1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/dest/file1.txt")
        .header("x-amz-copy-source", "test-bucket/test/file1.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp_copy_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

    let resp_copy1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_copy_body1))
        .unwrap();

    let req_copy2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/dest/file2.txt")
        .header("x-amz-copy-source", "test-bucket/test/file2.txt")
        .body(SdkBody::empty())
        .unwrap();

    let resp_copy_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

    let resp_copy2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_copy_body2))
        .unwrap();

    let req_delete = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/?delete")
        .body(SdkBody::empty())
        .unwrap();

    let resp_delete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file1.txt</Key>
            </Deleted>
            <Deleted>
                <Key>test/file2.txt</Key>
            </Deleted>
        </DeleteResult>"#;

    let resp_delete = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_delete_body))
        .unwrap();

    let events = vec![
        ReplayEvent::new(req_copy1, resp_copy1),
        ReplayEvent::new(req_copy2, resp_copy2),
        ReplayEvent::new(req_delete, resp_delete),
    ];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Move(S3Move {
        destination,
        flat: true,
        storage_class: None,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_restore_with_replay_client() -> Result<(), Error> {
    let key1 = "test/archive1.dat";
    let key2 = "test/archive2.dat";
    let key3 = "test/already_in_progress.dat";
    let key4 = "test/invalid_state.dat";

    let glacier_object = Object::builder()
        .e_tag("glacier-etag-1")
        .key(key1)
        .size(5000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let deeparchive_object = Object::builder()
        .e_tag("glacier-etag-2")
        .key(key2)
        .size(10000)
        .storage_class(ObjectStorageClass::DeepArchive)
        .last_modified(DateTime::from_str("2023-04-20T14:25:10.000Z", Format::DateTime).unwrap())
        .build();

    let restored_object = Object::builder()
        .e_tag("glacier-etag-3")
        .key(key3)
        .size(15000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-05-15T11:30:20.000Z", Format::DateTime).unwrap())
        .build();

    let invalid_state_object = Object::builder()
        .e_tag("glacier-etag-4")
        .key(key4)
        .size(20000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-06-25T16:45:30.000Z", Format::DateTime).unwrap())
        .build();

    let glacier_request = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/archive1.dat?restore")
        .body(SdkBody::empty()) // In a real request this would contain XML with restore parameters
        .unwrap();

    let glacier_response = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let deeparchive_request = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/archive2.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let deeparchive_response = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let request_with_inprogress = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/already_in_progress.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let inprogress_response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>RestoreAlreadyInProgress</Code>
            <Message>Object restore is already in progress</Message>
            <Resource>/test-bucket/test/already_in_progress.dat</Resource>
            <RequestId>EXAMPLE123456789</RequestId>
        </Error>"#;

    let response_with_inprogress_error = http::Response::builder()
        .status(StatusCode::CONFLICT)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(inprogress_response_body))
        .unwrap();

    let request_with_objectstate_error = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/invalid_state.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let invalid_objectstate_response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>InvalidObjectState</Code>
            <Message>The operation is not valid for the object's storage class</Message>
            <Resource>/test-bucket/test/invalid_state.dat</Resource>
            <RequestId>EXAMPLE987654321</RequestId>
        </Error>"#;

    let response_with_invalide_objectstate = http::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(invalid_objectstate_response_body))
        .unwrap();

    let events = vec![
        ReplayEvent::new(glacier_request, glacier_response),
        ReplayEvent::new(deeparchive_request, deeparchive_response),
        ReplayEvent::new(request_with_inprogress, response_with_inprogress_error),
        ReplayEvent::new(
            request_with_objectstate_error,
            response_with_invalide_objectstate,
        ),
    ];

    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::Restore(Restore {
        days: 7,
        tier: aws_sdk_s3::types::Tier::Standard,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(
        &client,
        &path,
        &[
            StreamObject::from_object(glacier_object),
            StreamObject::from_object(deeparchive_object),
            StreamObject::from_object(restored_object),
            StreamObject::from_object(invalid_state_object),
        ],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_restore_with_standard_tier() -> Result<(), Error> {
    let key = "test/standard_retrieval.dat";

    let glacier_object = Object::builder()
        .e_tag("glacier-etag")
        .key(key)
        .size(5000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let request_successful = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/standard_retrieval.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(request_successful, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let restore_cmd = Restore {
        days: 7,
        tier: aws_sdk_s3::types::Tier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Standard tier restore should succeed");

    Ok(())
}

#[tokio::test]
async fn test_restore_with_expedited_tier() -> Result<(), Error> {
    let key = "test/expedited_retrieval.dat";

    let glacier_object = Object::builder()
        .e_tag("glacier-etag")
        .key(key)
        .size(5000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/expedited_retrieval.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let restore_cmd = Restore {
        days: 2,
        tier: aws_sdk_s3::types::Tier::Expedited,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Expedited tier restore should succeed");

    Ok(())
}

#[tokio::test]
async fn test_restore_with_bulk_tier() -> Result<(), Error> {
    let key = "test/bulk_retrieval.dat";

    let deep_archive_object = Object::builder()
        .e_tag("deep-archive-etag")
        .key(key)
        .size(5000)
        .storage_class(ObjectStorageClass::DeepArchive)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/bulk_retrieval.dat?restore")
        .body(SdkBody::empty()) // In a real request this would contain XML with Bulk tier
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let restore_cmd = Restore {
        days: 30,
        tier: aws_sdk_s3::types::Tier::Bulk,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute(
            &client,
            &path,
            &[StreamObject::from_object(deep_archive_object)],
        )
        .await;
    assert!(result.is_ok(), "Bulk tier restore should succeed");

    Ok(())
}

#[tokio::test]
async fn test_restore_with_non_glacier_objects() -> Result<(), Error> {
    let key = "test/standard_object.dat";

    let standard_object = Object::builder()
        .e_tag("standard-etag")
        .key(key)
        .size(5000)
        .storage_class(ObjectStorageClass::Standard) // Standard storage, not Glacier
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let events: Vec<ReplayEvent> = vec![];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let restore_cmd = Restore {
        days: 7,
        tier: aws_sdk_s3::types::Tier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute(
            &client,
            &path,
            &[StreamObject::from_object(standard_object)],
        )
        .await;
    assert!(
        result.is_ok(),
        "Non-Glacier objects should be skipped without error"
    );

    Ok(())
}

#[tokio::test]
async fn test_restore_with_maximum_days() -> Result<(), Error> {
    let key = "test/long_term_retrieval.dat";

    let glacier_object = Object::builder()
        .e_tag("glacier-etag")
        .key(key)
        .size(5000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap())
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/test/long_term_retrieval.dat?restore")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let restore_cmd = Restore {
        days: 365,
        tier: aws_sdk_s3::types::Tier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Restore with maximum days should succeed");

    Ok(())
}

#[tokio::test]
async fn test_change_storage_class() -> Result<(), Error> {
    let key1 = "test/file1.txt";
    let key2 = "test/file2.txt";

    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key(key1)
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt")
        .header("x-amz-copy-source", "test-bucket/test/file1.txt")
        .header("x-amz-metadata-directive", "COPY")
        .header("x-amz-storage-class", "GLACIER")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body1))
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt")
        .header("x-amz-copy-source", "test-bucket/test/file2.txt")
        .header("x-amz-metadata-directive", "COPY")
        .header("x-amz-storage-class", "GLACIER")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body2))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::ChangeStorage(ChangeStorage {
        storage_class: StorageClass::Glacier,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    Ok(())
}

#[test]
fn test_object_record_with_version_fields() {
    use aws_sdk_s3::types::ObjectVersion;

    // Create a versioned object using StreamObject::from_version
    let version = ObjectVersion::builder()
        .key("versioned-file.txt")
        .version_id("ver123")
        .is_latest(true)
        .size(500)
        .build();

    let stream_obj = StreamObject::from_version(version);
    let record: ObjectRecord = (&stream_obj).into();

    assert_eq!(record.key, "versioned-file.txt");
    assert_eq!(record.version_id, Some("ver123".to_string()));
    assert_eq!(record.is_latest, Some(true));
    assert_eq!(record.is_delete_marker, None);
}

#[test]
fn test_object_record_with_delete_marker() {
    use aws_sdk_s3::types::DeleteMarkerEntry;

    let marker = DeleteMarkerEntry::builder()
        .key("deleted-file.txt")
        .version_id("del456")
        .is_latest(true)
        .build();

    let stream_obj = StreamObject::from_delete_marker(marker);
    let record: ObjectRecord = (&stream_obj).into();

    assert_eq!(record.key, "deleted-file.txt");
    assert_eq!(record.version_id, Some("del456".to_string()));
    assert_eq!(record.is_latest, Some(true));
    assert_eq!(record.is_delete_marker, Some(true));
}

#[test]
fn test_advanced_print_json_with_version_fields() -> Result<(), Error> {
    use aws_sdk_s3::types::ObjectVersion;
    use aws_sdk_s3::types::ObjectVersionStorageClass;

    let version = ObjectVersion::builder()
        .e_tag("ver-etag")
        .key("data/versioned.txt")
        .version_id("v1234")
        .is_latest(false)
        .size(1024)
        .storage_class(ObjectVersionStorageClass::Standard)
        .last_modified(DateTime::from_str("2023-06-15T10:30:00.000Z", Format::DateTime).unwrap())
        .build();

    let stream_obj = StreamObject::from_version(version);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, &stream_obj)?;

    let output = String::from_utf8(buf).unwrap();
    println!("JSON with version: {}", output);

    // Verify version fields are present in JSON output
    assert!(output.contains("\"version_id\":\"v1234\""));
    assert!(output.contains("\"is_latest\":false"));
    assert!(!output.contains("is_delete_marker")); // Should be omitted when false

    Ok(())
}

#[test]
fn test_advanced_print_json_with_delete_marker() -> Result<(), Error> {
    use aws_sdk_s3::types::DeleteMarkerEntry;

    let marker = DeleteMarkerEntry::builder()
        .key("data/deleted.txt")
        .version_id("dm789")
        .is_latest(true)
        .last_modified(DateTime::from_str("2023-07-20T14:00:00.000Z", Format::DateTime).unwrap())
        .build();

    let stream_obj = StreamObject::from_delete_marker(marker);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, &stream_obj)?;

    let output = String::from_utf8(buf).unwrap();
    println!("JSON with delete marker: {}", output);

    // Verify version and delete marker fields are present
    assert!(output.contains("\"version_id\":\"dm789\""));
    assert!(output.contains("\"is_latest\":true"));
    assert!(output.contains("\"is_delete_marker\":true"));

    Ok(())
}

#[tokio::test]
async fn test_delete_versioned_object() -> Result<(), Error> {
    use aws_sdk_s3::types::ObjectVersion;

    let version = ObjectVersion::builder()
        .e_tag("ver-etag")
        .key("test/versioned-file.txt")
        .version_id("v123abc")
        .is_latest(true)
        .size(100)
        .build();

    let stream_obj = StreamObject::from_version(version);

    // The delete request should include versionId
    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.amazonaws.com/?delete")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/versioned-file.txt</Key>
                <VersionId>v123abc</VersionId>
            </Deleted>
        </DeleteResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let cmd = Cmd::Delete(MultipleDelete {}).downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_copy_versioned_object() -> Result<(), Error> {
    use aws_sdk_s3::types::ObjectVersion;

    let version = ObjectVersion::builder()
        .e_tag("ver-etag")
        .key("source/file.txt")
        .version_id("srcver123")
        .is_latest(true)
        .size(500)
        .build();

    let stream_obj = StreamObject::from_version(version);

    // Copy request should include versionId in x-amz-copy-source
    let req = http::Request::builder()
        .method("PUT")
        .uri("https://dest-bucket.s3.amazonaws.com/dest/file.txt")
        .header(
            "x-amz-copy-source",
            "source-bucket/source/file.txt?versionId=srcver123",
        )
        .body(SdkBody::empty())
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    let destination = S3Path {
        bucket: "dest-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: None,
    })
    .downcast();

    let path = S3Path {
        bucket: "source-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;

    Ok(())
}

#[tokio::test]
async fn test_download_versioned_object() -> Result<(), Error> {
    use aws_sdk_s3::types::ObjectVersion;

    let version = ObjectVersion::builder()
        .e_tag("ver-etag")
        .key("downloads/file.txt")
        .version_id("dlver456")
        .is_latest(false)
        .size(100)
        .build();

    let stream_obj = StreamObject::from_version(version);

    // Download request should include versionId parameter
    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/downloads/file.txt?versionId=dlver456")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("text/plain"))
        .header("Content-Length", HeaderValue::from_static("13"))
        .body(SdkBody::from("file contents"))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_s3_test_credentials())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client.clone())
            .build(),
    );

    // Create a temp directory for download
    let temp_dir = std::env::temp_dir().join("s3find_test_versioned_download");
    std::fs::create_dir_all(&temp_dir)?;

    let cmd = Cmd::Download(Download {
        destination: temp_dir.to_string_lossy().to_string(),
        force: true,
    })
    .downcast();

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute(&client, &path, &[stream_obj]).await?;

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}
