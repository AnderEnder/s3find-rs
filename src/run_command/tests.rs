use super::*;
use crate::adapters::aws::ObjectHead;
use crate::adapters::aws::{DeleteObjectRequest, RestoreObjectStatus};
use crate::arg::*;
use crate::testing::{ExecuteCaptured, FakeS3Client, MultipartCall, replay_s3_client};
use aws_sdk_s3::primitives::{DateTimeFormat, SdkBody};
use aws_sdk_s3::{
    primitives::DateTime,
    types::{Object, ObjectStorageClass, Tag},
};
use aws_smithy_runtime::client::http::test_util::ReplayEvent;
use http::{HeaderValue, StatusCode};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
    let out = std::str::from_utf8(&buf)?;

    println!("{out}");
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
    let client = FakeS3Client::default();
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
        .execute_captured(&client, &path, &objects)
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
async fn test_multiple_delete_empty_input_skips_delete_request() -> Result<(), Error> {
    let client = FakeS3Client::default();
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };

    Cmd::Delete(MultipleDelete {})
        .execute_captured(&client, &path, &[])
        .await?;

    assert!(client.delete_calls.lock().unwrap().is_empty());

    Ok(())
}

#[tokio::test]
async fn test_multiple_delete_missing_key_returns_error() {
    let client = FakeS3Client::default();
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject::from_object(Object::builder().build())];

    let err = Cmd::Delete(MultipleDelete {})
        .execute_captured(&client, &path, &objects)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("S3 Object is not complete"));
    assert!(client.delete_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_set_tags_with_fake_client_records_tags() -> Result<(), Error> {
    let client = FakeS3Client::default();
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
    .execute_captured(&client, &path, &objects)
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
    let client = FakeS3Client::default().with_tags(
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
        .execute_captured(&client, &path, &objects)
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
    let client = FakeS3Client::default().with_object_body(
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
    .execute_captured(&client, &path, &objects)
    .await?;

    let file_path = temp_dir.path().join("test/file.txt.vv1");
    assert_eq!(fs::read_to_string(file_path)?, "hello world");
    assert_eq!(client.get_object_calls.lock().unwrap().len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_s3_copy_with_fake_client_records_request() -> Result<(), Error> {
    let client = FakeS3Client::default();
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
        storage_class: Some(StorageClassName::from("GLACIER")),
    })
    .execute_captured(&client, &path, &objects)
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
    let client = FakeS3Client::default();
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
        storage_class: Some(StorageClassName::from("STANDARD_IA")),
    })
    .execute_captured(&client, &path, &objects)
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
async fn test_s3_move_delete_markers_only_skips_delete_request() -> Result<(), Error> {
    let client = FakeS3Client::default();
    let path = S3Path {
        bucket: "source-bucket".to_string(),
        prefix: None,
    };
    let objects = vec![StreamObject {
        object: Object::builder().key("source/file.txt").build(),
        version_id: None,
        is_latest: None,
        is_delete_marker: true,
        tags: None,
    }];

    Cmd::Move(S3Move {
        destination: S3Path {
            bucket: "dest-bucket".to_string(),
            prefix: Some("archive".to_string()),
        },
        flat: false,
        storage_class: Some(StorageClassName::from("STANDARD_IA")),
    })
    .execute_captured(&client, &path, &objects)
    .await?;

    assert!(client.copy_calls.lock().unwrap().is_empty());
    assert!(client.delete_calls.lock().unwrap().is_empty());

    Ok(())
}

#[tokio::test]
async fn test_restore_with_fake_client_records_request() -> Result<(), Error> {
    let client = FakeS3Client::default().with_restore_status(
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
        tier: RestoreTier::Bulk,
    })
    .execute_captured(&client, &path, &objects)
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

    println!("Output with owner, no date, no storage: {out}");
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
    let cmd = FastPrint::default();
    let bucket = "test";

    let object = Object::builder()
        .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
        .key("somepath/otherpath")
        .size(4_997_288)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(DateTime::from_str(
            "2017-07-19T19:04:17.000Z",
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
    let out = std::str::from_utf8(&buf)?;

    assert!(out.contains("s3://test/somepath/otherpath"));
    Ok(())
}

#[tokio::test]
async fn test_exec() -> Result<(), Error> {
    let cmd = Exec {
        utility: "echo test {}".to_owned(),
    };

    let path = "s3://test/somepath/otherpath";
    let out = exec::run(&cmd.command(path)?).await?;

    assert!(out.contains("test"));
    assert!(out.contains("s3://test/somepath/otherpath"));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_supports_quoted_arguments() -> Result<(), Error> {
    let cmd = Exec {
        utility: "echo \"test {}\"".to_owned(),
    };

    let path = "s3://test/somepath/otherpath";
    let out = exec::run(&cmd.command(path)?).await?;

    assert_eq!(out.trim(), format!("test {path}"));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_propagates_non_zero_status() {
    let cmd = Exec {
        utility: "sh -c 'exit 7'".to_owned(),
    };

    let err = exec::run(&cmd.command("ignored").unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed with status 7"));
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_propagates_stderr_output() {
    let cmd = Exec {
        utility: "sh -c 'echo boom 1>&2; exit 7'".to_owned(),
    };

    let err = exec::run(&cmd.command("ignored").unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed with status 7: boom"));
}

#[cfg(windows)]
#[tokio::test]
async fn test_exec_propagates_non_zero_status() {
    let cmd = Exec {
        utility: "cmd /C exit 7".to_owned(),
    };

    let err = exec::run(&cmd.command("ignored").unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed with status 7"));
}

#[cfg(windows)]
#[tokio::test]
async fn test_exec_propagates_stderr_output() {
    let cmd = Exec {
        utility: "cmd /C \"echo boom 1>&2 && exit 7\"".to_owned(),
    };

    let err = exec::run(&cmd.command("ignored").unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed with status 7: boom"));
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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, "test-bucket", &stream_obj)?;

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
            DateTimeFormat::DateTime,
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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Csv,
    };

    cmd.print_csv_stream_objects(&mut buf, &[stream_obj1.clone(), stream_obj2.clone()])?;

    let output = String::from_utf8(buf.clone()).unwrap();
    println!("CSV Output:\n{output}");

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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Print(AdvancedPrint::default());
    let client = FakeS3Client::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;
    Ok(())
}

#[tokio::test]
async fn smoke_advanced_print_json_execute() -> Result<(), Error> {
    let object1 = Object::builder()
        .e_tag("test-etag-1")
        .key("folder/file1.txt")
        .size(100)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key("folder/file2.txt")
        .size(200)
        .storage_class(ObjectStorageClass::IntelligentTiering)
        .last_modified(
            DateTime::from_str("2023-02-15T12:30:45.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let cmd = Cmd::Print(AdvancedPrint {
        format: PrintFormat::Json,
    });

    let client = FakeS3Client::default();
    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("csv-etag-2")
        .key("data/archive.zip")
        .size(10000)
        .storage_class(ObjectStorageClass::DeepArchive)
        .last_modified(
            DateTime::from_str("2023-04-20T14:25:10.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let cmd = Cmd::Print(AdvancedPrint {
        format: PrintFormat::Csv,
    });

    let client = FakeS3Client::default();
    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Ls(FastPrint::default());
    let client = FakeS3Client::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;
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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Nothing(DoNothing {});
    let client = FakeS3Client::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    let out = cmd.execute_captured(&client, &path, &[stream_obj]).await?;
    assert!(out.is_empty());
    Ok(())
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
            DateTimeFormat::DateTime,
        )?)
        .build();
    let stream_obj = StreamObject::from_object(object);

    let cmd = Cmd::Exec(Exec {
        utility: "echo {}".to_owned(),
    });

    let client = FakeS3Client::default();

    let path = S3Path {
        bucket: "test".to_owned(),
        prefix: None,
    };

    let out = cmd.execute_captured(&client, &path, &[stream_obj]).await?;
    assert_eq!(out.trim(), "s3://test/somepath/otherpath");
    Ok(())
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj = StreamObject::from_object(object);

    let content = "Test content\n";

    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file.txt?x-id=GetObject")
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

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::Download(Download {
        destination: temp_path.clone(),
        force: true,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    let file_path = PathBuf::from(temp_path).join(key);
    assert!(file_path.exists(), "Downloaded file should exist");

    let downloaded_content = fs::read_to_string(&file_path)?;
    assert_eq!(downloaded_content, "Test content\n");

    replay.assert_requests_match(&[]);
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/new.txt?x-id=GetObject")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("text/plain"))
        .header("Content-Length", HeaderValue::from_static("12"))
        .body(SdkBody::from("new content\n"))
        .unwrap();

    let (client, replay) = replay_s3_client(vec![ReplayEvent::new(req, resp)]);

    let cmd = Cmd::Download(Download {
        destination: temp_path.clone(),
        force: false,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj_existing, stream_obj_new])
        .await?;

    assert_eq!(std::fs::read_to_string(existing_path)?, "keep me");
    assert_eq!(
        std::fs::read_to_string(PathBuf::from(temp_path).join(new_key))?,
        "new content\n"
    );

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj1 = StreamObject::from_object(object1);

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();
    let stream_obj2 = StreamObject::from_object(object2);

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file1.txt?acl")
        .body(SdkBody::empty())
        .unwrap();

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file2.txt?acl")
        .body(SdkBody::empty())
        .unwrap();

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::Public(SetPublic {});

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
    Ok(())
}

#[tokio::test]
async fn test_set_public_defaults_region_when_client_region_is_missing() -> Result<(), Error> {
    let client = FakeS3Client {
        region: None,
        ..Default::default()
    };
    let stream_obj = StreamObject::from_object(Object::builder().key("test/file.txt").build());
    let cmd = Cmd::Public(SetPublic {});
    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    assert_eq!(
        client.public_calls.lock().unwrap().as_slice(),
        &[FakeS3Client::object_key(
            "test-bucket",
            "test/file.txt",
            None
        )]
    );
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/file1.txt</Key></Object><Object><Key>test/file2.txt</Key></Object></Delete>"#,
        ))
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

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::Delete(MultipleDelete {});

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
    Ok(())
}

#[tokio::test]
async fn test_multiple_delete_propagates_request_failure() {
    let object = Object::builder().key("test/file1.txt").size(100).build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/file1.txt</Key></Object></Delete>"#,
        ))
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

    let (client, replay) = replay_s3_client(vec![ReplayEvent::new(req, resp)]);

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = Cmd::Delete(MultipleDelete {})
        .execute_captured(&client, &path, &[StreamObject::from_object(object)])
        .await;

    assert!(result.is_err());
    replay.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_multiple_delete_reports_deleted_keys_when_others_fail() {
    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/file1.txt</Key></Object><Object><Key>test/file2.txt</Key></Object></Delete>"#,
        ))
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
                <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Deleted>
                        <Key>test/file1.txt</Key>
                    </Deleted>
                    <Error>
                        <Key>test/file2.txt</Key>
                        <VersionId>v2</VersionId>
                        <Code>AccessDenied</Code>
                        <Message>Access Denied</Message>
                    </Error>
                </DeleteResult>"#,
        ))
        .unwrap();

    let (client, replay) = replay_s3_client(vec![ReplayEvent::new(req, resp)]);
    let objects = [object("test/file1.txt"), object("test/file2.txt")];

    let path = test_path();
    let mut out = Vec::new();
    let mut ctx = CommandContext::new(&client, &path, &mut out, CommandOptions::default());
    let err = MultipleDelete {}
        .execute(&mut ctx, &objects)
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "failed to delete 1 of the objects: test/file2.txt?versionId=v2: AccessDenied (Access Denied)"
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "deleted: s3://test-bucket/test/file1.txt\n"
    );
    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file1.txt?tagging")
        .body(SdkBody::from(
            r#"<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>tag1</Key><Value>value1</Value></Tag><Tag><Key>tag2</Key><Value>value2</Value></Tag></TagSet></Tagging>"#,
        ))
        .unwrap();

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let req2 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file2.txt?tagging")
        .body(SdkBody::from(
            r#"<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>tag1</Key><Value>value1</Value></Tag><Tag><Key>tag2</Key><Value>value2</Value></Tag></TagSet></Tagging>"#,
        ))
        .unwrap();

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

    let (client, replay) = replay_s3_client(events);

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

    let cmd = Cmd::Tags(SetTags { tags });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
    Ok(())
}

#[tokio::test]
async fn test_set_tags_rejects_too_many_tags() {
    let (client, replay) = replay_s3_client(vec![]);

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };
    let objects = vec![StreamObject::from_object(
        Object::builder().key("test/file1.txt").build(),
    )];

    let result = Cmd::Tags(SetTags {
        tags: (0..11)
            .map(|i| FindTag {
                key: format!("tag{i}"),
                value: "value".to_string(),
            })
            .collect(),
    })
    .execute_captured(&client, &path, &objects)
    .await;

    assert!(result.is_err());
    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req1 = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file1.txt?tagging")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file2.txt?tagging")
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

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::LsTags(ListTags {});

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    // Request includes versionId query parameter
    let req1 = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/versioned.txt?tagging&versionId=v123")
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

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::LsTags(ListTags {});

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

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    // No HTTP events needed - tags are cached
    let (client, replay) = replay_s3_client(vec![]);

    let cmd = Cmd::LsTags(ListTags {});

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
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    // No HTTP events needed - delete marker should be skipped
    let (client, replay) = replay_s3_client(vec![]);

    let cmd = Cmd::LsTags(ListTags {});

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
    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/dest/file1.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile1.txt")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/dest/file2.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile2.txt")
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

    let (client, replay) = replay_s3_client(events);

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: None,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/archive/file_to_copy.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile_to_copy.txt")
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
    let (client, replay) = replay_s3_client(events);

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("archive/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: Some(StorageClassName::from("GLACIER")),
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj = StreamObject::from_object(object);
    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req_copy = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/archive/file_to_move.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile_to_move.txt")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/file_to_move.txt</Key></Object></Delete>"#,
        ))
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

    let (client, replay) = replay_s3_client(events);

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("archive/".to_owned()),
    };

    let cmd = Cmd::Move(S3Move {
        destination,
        flat: true,
        storage_class: Some(StorageClassName::from("INTELLIGENT_TIERING")),
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj = StreamObject::from_object(object);
    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req_copy1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/dest/file1.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile1.txt")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/dest/file2.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile2.txt")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/file1.txt</Key></Object><Object><Key>test/file2.txt</Key></Object></Delete>"#,
        ))
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

    let (client, replay) = replay_s3_client(events);

    let destination = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Move(S3Move {
        destination,
        flat: true,
        storage_class: None,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    cmd.execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let deeparchive_object = Object::builder()
        .e_tag("glacier-etag-2")
        .key(key2)
        .size(10000)
        .storage_class(ObjectStorageClass::DeepArchive)
        .last_modified(
            DateTime::from_str("2023-04-20T14:25:10.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let restored_object = Object::builder()
        .e_tag("glacier-etag-3")
        .key(key3)
        .size(15000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(
            DateTime::from_str("2023-05-15T11:30:20.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let invalid_state_object = Object::builder()
        .e_tag("glacier-etag-4")
        .key(key4)
        .size(20000)
        .storage_class(ObjectStorageClass::Glacier)
        .last_modified(
            DateTime::from_str("2023-06-25T16:45:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let glacier_request = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/archive1.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let glacier_response = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let deeparchive_request = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/archive2.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let deeparchive_response = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let request_with_inprogress = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/already_in_progress.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/invalid_state.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
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

    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::Restore(Restore {
        days: 7,
        tier: RestoreTier::Standard,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(
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

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let request_successful = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/standard_retrieval.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(request_successful, resp)];
    let (client, replay) = replay_s3_client(events);

    let restore_cmd = Restore {
        days: 7,
        tier: RestoreTier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute_captured(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Standard tier restore should succeed");

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/expedited_retrieval.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>2</Days><GlacierJobParameters><Tier>Expedited</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let (client, replay) = replay_s3_client(events);

    let restore_cmd = Restore {
        days: 2,
        tier: RestoreTier::Expedited,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute_captured(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Expedited tier restore should succeed");

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/bulk_retrieval.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>30</Days><GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let (client, replay) = replay_s3_client(events);

    let restore_cmd = Restore {
        days: 30,
        tier: RestoreTier::Bulk,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute_captured(
            &client,
            &path,
            &[StreamObject::from_object(deep_archive_object)],
        )
        .await;
    assert!(result.is_ok(), "Bulk tier restore should succeed");

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let events: Vec<ReplayEvent> = vec![];
    let (client, replay) = replay_s3_client(events);

    let restore_cmd = Restore {
        days: 7,
        tier: RestoreTier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute_captured(
            &client,
            &path,
            &[StreamObject::from_object(standard_object)],
        )
        .await;
    assert!(
        result.is_ok(),
        "Non-Glacier objects should be skipped without error"
    );

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-03-10T09:15:30.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req = http::Request::builder()
        .method("POST")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/long_term_retrieval.dat?restore")
        .body(SdkBody::from(
            r#"<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>365</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"#,
        ))
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(SdkBody::empty())
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let (client, replay) = replay_s3_client(events);

    let restore_cmd = Restore {
        days: 365,
        tier: RestoreTier::Standard,
    };

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let result = restore_cmd
        .execute_captured(&client, &path, &[StreamObject::from_object(glacier_object)])
        .await;
    assert!(result.is_ok(), "Restore with maximum days should succeed");

    replay.assert_requests_match(&[]);
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
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let object2 = Object::builder()
        .e_tag("test-etag-2")
        .key(key2)
        .size(200)
        .storage_class(ObjectStorageClass::Standard)
        .last_modified(
            DateTime::from_str("2023-01-01T00:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let req1 = http::Request::builder()
        .method("PUT")
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file1.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile1.txt")
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/test/file2.txt?x-id=CopyObject")
        .header("x-amz-copy-source", "test-bucket/test%2Ffile2.txt")
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
    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::ChangeStorage(ChangeStorage {
        storage_class: StorageClassName::from("GLACIER"),
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    let stream_obj1 = StreamObject::from_object(object1);
    let stream_obj2 = StreamObject::from_object(object2);
    let output = cmd
        .execute_captured(&client, &path, &[stream_obj1, stream_obj2])
        .await?;

    assert_eq!(
        output,
        "Changing storage class for s3://test-bucket/test/file1.txt to GLACIER\n\
         Changing storage class for s3://test-bucket/test/file2.txt to GLACIER\n"
    );
    replay.assert_requests_match(&[]);
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
    assert_eq!(record.version_id.as_deref(), Some("ver123"));
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
    assert_eq!(record.version_id.as_deref(), Some("del456"));
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
        .last_modified(
            DateTime::from_str("2023-06-15T10:30:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let stream_obj = StreamObject::from_version(version);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, "test-bucket", &stream_obj)?;

    let output = String::from_utf8(buf).unwrap();
    println!("JSON with version: {output}");

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
        .last_modified(
            DateTime::from_str("2023-07-20T14:00:00.000Z", DateTimeFormat::DateTime).unwrap(),
        )
        .build();

    let stream_obj = StreamObject::from_delete_marker(marker);

    let mut buf = Vec::<u8>::new();
    let cmd = AdvancedPrint {
        format: PrintFormat::Json,
    };

    cmd.print_json_stream_object(&mut buf, "test-bucket", &stream_obj)?;

    let output = String::from_utf8(buf).unwrap();
    println!("JSON with delete marker: {output}");

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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/?delete")
        .body(SdkBody::from(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>test/versioned-file.txt</Key><VersionId>v123abc</VersionId></Object></Delete>"#,
        ))
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
    let (client, replay) = replay_s3_client(events);

    let cmd = Cmd::Delete(MultipleDelete {});

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .uri("https://dest-bucket.s3.us-east-1.amazonaws.com/dest/file.txt?x-id=CopyObject")
        .header(
            "x-amz-copy-source",
            "source-bucket/source%2Ffile.txt?versionId=srcver123",
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
    let (client, replay) = replay_s3_client(events);

    let destination = S3Path {
        bucket: "dest-bucket".to_owned(),
        prefix: Some("dest/".to_owned()),
    };

    let cmd = Cmd::Copy(S3Copy {
        destination,
        flat: true,
        storage_class: None,
    });

    let path = S3Path {
        bucket: "source-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    replay.assert_requests_match(&[]);
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
        .uri("https://test-bucket.s3.us-east-1.amazonaws.com/downloads/file.txt?x-id=GetObject&versionId=dlver456")
        .body(SdkBody::empty())
        .unwrap();

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("text/plain"))
        .header("Content-Length", HeaderValue::from_static("13"))
        .body(SdkBody::from("file contents"))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let (client, replay) = replay_s3_client(events);

    // Create a temp directory for download
    let temp_dir = std::env::temp_dir().join("s3find_test_versioned_download");
    std::fs::create_dir_all(&temp_dir)?;

    let cmd = Cmd::Download(Download {
        destination: temp_dir.to_string_lossy().to_string(),
        force: true,
    });

    let path = S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: None,
    };

    cmd.execute_captured(&client, &path, &[stream_obj]).await?;

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);

    replay.assert_requests_match(&[]);
    Ok(())
}

/// `ls-tags` over three objects whose tag lookups take 300, 200 and 100ms, so
/// they finish in reverse order when run concurrently. Returns the output and
/// the (paused-clock) time it took.
async fn list_tags_with_delays(concurrency: usize) -> (String, Duration) {
    let keys = [("a.txt", 300), ("b.txt", 200), ("c.txt", 100)];
    let client = keys
        .iter()
        .fold(FakeS3Client::default(), |client, &(key, millis)| {
            client.with_tag_delay("test-bucket", key, millis)
        });
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects: Vec<_> = keys
        .iter()
        .map(|(key, _)| StreamObject::from_object(Object::builder().key(*key).build()))
        .collect();

    let start = tokio::time::Instant::now();
    let output = ListTags {}
        .execute_with(
            &client,
            &path,
            &objects,
            CommandOptions {
                concurrency,
                ..CommandOptions::default()
            },
        )
        .await
        .unwrap();
    (output, start.elapsed())
}

#[tokio::test(start_paused = true)]
async fn test_per_object_requests_run_concurrently_in_input_order() {
    let expected = "s3://test-bucket/a.txt \ns3://test-bucket/b.txt \ns3://test-bucket/c.txt \n";

    let (output, elapsed) = list_tags_with_delays(3).await;
    assert_eq!(output, expected);
    assert_eq!(elapsed, Duration::from_millis(300));

    let (output, elapsed) = list_tags_with_delays(1).await;
    assert_eq!(output, expected);
    assert_eq!(elapsed, Duration::from_millis(600));
}

#[tokio::test]
async fn test_per_object_request_errors_name_the_object() {
    let client = FakeS3Client::default().with_tag_error("test-bucket", "b.txt", 403);
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    };
    let objects: Vec<_> = ["a.txt", "b.txt", "c.txt"]
        .into_iter()
        .map(|key| StreamObject::from_object(Object::builder().key(key).build()))
        .collect();

    let err = ListTags {}
        .execute_captured(&client, &path, &objects)
        .await
        .unwrap_err();

    assert_eq!(
        format!("{err:#}"),
        "s3://test-bucket/b.txt: Access denied for s3:GetObjectTagging on test-bucket/b.txt"
    );
}

fn test_path() -> S3Path {
    S3Path {
        bucket: "test-bucket".to_string(),
        prefix: None,
    }
}

/// An object in Glacier, so every command, including restore, acts on it.
fn archived_object() -> StreamObject {
    StreamObject::from_object(
        Object::builder()
            .key("dir/file.txt")
            .size(4)
            .storage_class(ObjectStorageClass::Glacier)
            .build(),
    )
}

/// Every command that changes objects, with its `--dry-run` text output for
/// `archived_object()`.
fn changing_commands(download_dir: &std::path::Path) -> Vec<(Cmd, String)> {
    let download_path = download_dir.join("dir/file.txt");
    let destination = S3Path {
        bucket: "dest-bucket".to_string(),
        prefix: Some("backup".to_string()),
    };
    vec![
        (
            Cmd::Delete(MultipleDelete {}),
            "deleted: s3://test-bucket/dir/file.txt".to_string(),
        ),
        (
            Cmd::Copy(S3Copy {
                destination: destination.clone(),
                flat: false,
                storage_class: None,
            }),
            "copying: s3://test-bucket/dir%2Ffile.txt => s3://dest-bucket/backup/dir/file.txt"
                .to_string(),
        ),
        (
            Cmd::Move(S3Move {
                destination,
                flat: true,
                storage_class: None,
            }),
            "moving: s3://test-bucket/dir%2Ffile.txt => s3://dest-bucket/backup/file.txt"
                .to_string(),
        ),
        (
            Cmd::Tags(SetTags {
                tags: vec![FindTag {
                    key: "env".to_string(),
                    value: "prod".to_string(),
                }],
            }),
            "tags are set for: s3://test-bucket/dir/file.txt".to_string(),
        ),
        (
            Cmd::Public(SetPublic {}),
            "dir/file.txt https://test-bucket.s3.amazonaws.com/dir/file.txt".to_string(),
        ),
        (
            Cmd::Restore(Restore::default()),
            "Restore initiated for: dir/file.txt".to_string(),
        ),
        (
            Cmd::ChangeStorage(ChangeStorage {
                storage_class: StorageClassName::from("DEEP_ARCHIVE"),
            }),
            "Changing storage class for s3://test-bucket/dir/file.txt to DEEP_ARCHIVE".to_string(),
        ),
        (
            Cmd::Download(Download {
                destination: download_dir.to_string_lossy().to_string(),
                force: false,
            }),
            format!(
                "downloading: s3://test-bucket/dir/file.txt => {}",
                download_path.display()
            ),
        ),
        (
            // Fails if it is run.
            Cmd::Exec(Exec {
                utility: "false {}".to_string(),
            }),
            "false s3://test-bucket/dir/file.txt".to_string(),
        ),
    ]
}

fn dry_run(json: bool) -> CommandOptions {
    CommandOptions {
        json,
        dry_run: true,
        ..CommandOptions::default()
    }
}

/// Whether `client` recorded any call that changes or reads object data.
fn made_changes(client: &FakeS3Client) -> bool {
    !(client.delete_calls.lock().unwrap().is_empty()
        && client.put_tag_calls.lock().unwrap().is_empty()
        && client.public_calls.lock().unwrap().is_empty()
        && client.copy_calls.lock().unwrap().is_empty()
        && client.restore_calls.lock().unwrap().is_empty()
        && client.multipart_calls.lock().unwrap().is_empty()
        && client.get_object_calls.lock().unwrap().is_empty())
}

#[tokio::test]
async fn test_dry_run_changes_nothing_and_marks_output() {
    let download_dir = tempdir().unwrap();

    for (cmd, line) in changing_commands(download_dir.path()) {
        let client = FakeS3Client::default();
        let output = cmd
            .execute_with(&client, &test_path(), &[archived_object()], dry_run(false))
            .await
            .unwrap();

        assert_eq!(output, format!("[dry-run] {line}\n"), "{cmd:?}");
        assert!(!made_changes(&client), "{cmd:?} made changes");
    }
    assert_eq!(fs::read_dir(download_dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn test_dry_run_json_marks_each_record() {
    let download_dir = tempdir().unwrap();

    for (cmd, _) in changing_commands(download_dir.path()) {
        let client = FakeS3Client::default();
        let output = cmd
            .execute_with(&client, &test_path(), &[archived_object()], dry_run(true))
            .await
            .unwrap();

        let record: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(record["dry_run"], true, "{cmd:?}: {output}");
        assert_eq!(record["key"], "dir/file.txt", "{cmd:?}: {output}");
        assert_eq!(record["url"], "s3://test-bucket/dir/file.txt", "{cmd:?}");
        assert!(record["action"].is_string(), "{cmd:?}: {output}");
        assert!(!made_changes(&client), "{cmd:?} made changes");
    }
}

#[tokio::test]
async fn test_dry_run_leaves_read_only_commands_unmarked() {
    let client = FakeS3Client::default();
    let commands = [
        Cmd::Ls(FastPrint::default()),
        Cmd::Print(AdvancedPrint::default()),
        Cmd::LsTags(ListTags {}),
    ];

    for cmd in commands {
        let output = cmd
            .execute_with(&client, &test_path(), &[archived_object()], dry_run(false))
            .await
            .unwrap();
        assert!(output.contains("dir/file.txt"), "{cmd:?}: {output}");
        assert!(!output.contains("dry-run"), "{cmd:?}: {output}");
    }
}

/// Runs `cmd` on `objects` with `--json` and parses each output line.
async fn json_lines(
    cmd: &Cmd,
    client: &FakeS3Client,
    objects: &[StreamObject],
) -> Vec<serde_json::Value> {
    let options = CommandOptions {
        json: true,
        ..CommandOptions::default()
    };
    cmd.execute_with(client, &test_path(), objects, options)
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn test_json_output_of_commands_that_change_objects() {
    let client = FakeS3Client::default().with_restore_status(
        "test-bucket",
        "dir/file.txt",
        None,
        RestoreObjectStatus::AlreadyInProgress,
    );
    let mut versioned = archived_object();
    versioned.version_id = Some("v1".to_string());

    let delete = json_lines(&Cmd::Delete(MultipleDelete {}), &client, &[versioned]).await;
    assert_eq!(
        delete,
        [serde_json::json!({
            "action": "delete",
            "bucket": "test-bucket",
            "key": "dir/file.txt",
            "version_id": "v1",
            "url": "s3://test-bucket/dir/file.txt",
        })]
    );

    let copy = Cmd::Copy(S3Copy {
        destination: S3Path {
            bucket: "dest-bucket".to_string(),
            prefix: None,
        },
        flat: true,
        storage_class: None,
    });
    assert_eq!(
        json_lines(&copy, &client, &[archived_object()]).await,
        [serde_json::json!({
            "action": "copy",
            "bucket": "test-bucket",
            "key": "dir/file.txt",
            "url": "s3://test-bucket/dir/file.txt",
            "destination": "s3://dest-bucket/file.txt",
        })]
    );

    let tags = Cmd::Tags(SetTags {
        tags: vec![FindTag {
            key: "env".to_string(),
            value: "prod".to_string(),
        }],
    });
    assert_eq!(
        json_lines(&tags, &client, &[archived_object()]).await[0]["tags"],
        serde_json::json!({"env": "prod"})
    );

    let restore = json_lines(
        &Cmd::Restore(Restore::default()),
        &client,
        &[archived_object()],
    )
    .await;
    assert_eq!(restore[0]["action"], "restore");
    assert_eq!(restore[0]["status"], "already_in_progress");

    let public = json_lines(&Cmd::Public(SetPublic {}), &client, &[archived_object()]).await;
    assert_eq!(
        public[0]["public_url"],
        "https://test-bucket.s3.amazonaws.com/dir/file.txt"
    );
}

#[tokio::test]
async fn test_json_output_of_read_only_commands() {
    let mut tagged = archived_object();
    tagged.tags = Some(vec![
        Tag::builder().key("env").value("prod").build().unwrap(),
    ]);
    let client = FakeS3Client::default();

    for cmd in [
        Cmd::Ls(FastPrint::default()),
        Cmd::Print(AdvancedPrint {
            format: PrintFormat::Csv,
        }),
    ] {
        let records = json_lines(&cmd, &client, std::slice::from_ref(&tagged)).await;
        assert_eq!(records.len(), 1, "{cmd:?}");
        assert_eq!(records[0]["bucket"], "test-bucket");
        assert_eq!(records[0]["key"], "dir/file.txt");
        assert_eq!(records[0]["url"], "s3://test-bucket/dir/file.txt");
        assert_eq!(records[0]["size"], 4);
        assert_eq!(records[0]["storage_class"], "GLACIER");
    }

    assert_eq!(
        json_lines(&Cmd::LsTags(ListTags {}), &client, &[tagged]).await,
        [serde_json::json!({
            "bucket": "test-bucket",
            "key": "dir/file.txt",
            "url": "s3://test-bucket/dir/file.txt",
            "tags": {"env": "prod"},
        })]
    );
}

#[tokio::test]
async fn test_download_reports_skipped_files() -> Result<(), Error> {
    let download_dir = tempdir()?;
    let existing = download_dir.path().join("dir/file.txt");
    fs::create_dir_all(existing.parent().unwrap())?;
    fs::write(&existing, "old")?;
    let cmd = Cmd::Download(Download {
        destination: download_dir.path().to_string_lossy().to_string(),
        force: false,
    });
    let client = FakeS3Client::default();

    // Text goes to stderr, so stdout stays empty; JSON reports the skip.
    assert_eq!(
        cmd.execute_captured(&client, &test_path(), &[archived_object()])
            .await?,
        ""
    );
    let records = json_lines(&cmd, &client, &[archived_object()]).await;
    assert_eq!(records[0]["action"], "download");
    assert_eq!(records[0]["skipped"], true);
    assert_eq!(records[0]["path"], existing.to_string_lossy().as_ref());
    assert!(client.get_object_calls.lock().unwrap().is_empty());
    assert_eq!(fs::read_to_string(existing)?, "old");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_json_includes_command_and_stdout() {
    let cmd = Cmd::Exec(Exec {
        utility: "echo found {}".to_string(),
    });
    let records = json_lines(&cmd, &FakeS3Client::default(), &[archived_object()]).await;

    assert_eq!(
        records,
        [serde_json::json!({
            "action": "exec",
            "bucket": "test-bucket",
            "key": "dir/file.txt",
            "url": "s3://test-bucket/dir/file.txt",
            "command": "echo found s3://test-bucket/dir/file.txt",
            "stdout": "found s3://test-bucket/dir/file.txt\n",
        })]
    );
}

fn object(key: &str) -> StreamObject {
    StreamObject::from_object(Object::builder().key(key).size(4).build())
}

fn version(key: &str, version_id: &str) -> StreamObject {
    let mut object = object(key);
    object.version_id = Some(version_id.to_owned());
    object.is_latest = Some(true);
    object
}

fn download_to(destination: &std::path::Path, force: bool) -> Cmd {
    Cmd::Download(Download {
        destination: destination.to_string_lossy().to_string(),
        force,
    })
}

#[tokio::test]
async fn test_download_refuses_keys_outside_the_destination() -> Result<(), Error> {
    let root = tempdir()?;
    let cmd = download_to(&root.path().join("downloads"), true);

    for key in ["../escaped.txt", "dir/../../escaped.txt", "/escaped.txt"] {
        let client = FakeS3Client::default().with_object_body("test-bucket", key, None, b"data");
        let err = cmd
            .execute_captured(&client, &test_path(), &[object(key)])
            .await
            .unwrap_err();

        assert!(
            format!("{err:#}").contains("refusing to download outside"),
            "{key}: {err:#}"
        );
        assert!(client.get_object_calls.lock().unwrap().is_empty(), "{key}");
    }
    assert!(!root.path().join("escaped.txt").exists());
    Ok(())
}

#[tokio::test]
async fn test_download_turns_folder_markers_into_directories() -> Result<(), Error> {
    let dir = tempdir()?;
    let client =
        FakeS3Client::default().with_object_body("test-bucket", "photos/a.jpg", None, b"jpg");
    // A versioned marker is still the folder, not a file inside it.
    let objects = [
        object("photos/"),
        object("photos/a.jpg"),
        version("empty/", "v1"),
    ];

    let out = download_to(dir.path(), false)
        .execute_captured(&client, &test_path(), &objects)
        .await?;

    assert!(dir.path().join("empty").is_dir());
    assert_eq!(fs::read_dir(dir.path().join("empty"))?.count(), 0);
    assert_eq!(fs::read(dir.path().join("photos/a.jpg"))?, b"jpg");
    assert_eq!(client.get_object_calls.lock().unwrap().len(), 1);
    assert_eq!(out.lines().count(), 3);
    Ok(())
}

#[tokio::test]
async fn test_failed_download_leaves_no_partial_file() -> Result<(), Error> {
    let dir = tempdir()?;
    // A directory where the file belongs makes moving the download into place fail.
    fs::create_dir_all(dir.path().join("file.txt/inner"))?;
    let client = FakeS3Client::default().with_object_body("test-bucket", "file.txt", None, b"data");

    let result = download_to(dir.path(), true)
        .execute_captured(&client, &test_path(), &[object("file.txt")])
        .await;

    assert!(result.is_err());
    let names: Vec<_> = fs::read_dir(dir.path())?
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names, ["file.txt"]);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_downloaded_files_get_default_permissions() -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir()?;
    let client = FakeS3Client::default().with_object_body("test-bucket", "file.txt", None, b"data");
    download_to(dir.path(), false)
        .execute_captured(&client, &test_path(), &[object("file.txt")])
        .await?;

    let created = dir.path().join("created.txt");
    fs::File::create(&created)?;
    let mode = |path: PathBuf| fs::metadata(path).map(|metadata| metadata.permissions().mode());
    assert_eq!(mode(dir.path().join("file.txt"))?, mode(created)?);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_passes_each_target_as_one_argument() -> Result<(), Error> {
    let key = r#"dir/it's a "file".txt"#;
    let cmd = Cmd::Exec(Exec {
        utility: "printf '<%s>' {}".to_owned(),
    });
    let client = FakeS3Client::default();

    let out = cmd
        .execute_captured(&client, &test_path(), &[object(key)])
        .await?;
    assert_eq!(out, format!("<s3://test-bucket/{key}>\n"));

    // The reported command line, pasted into a shell, runs the same arguments.
    let preview = cmd
        .execute_with(&client, &test_path(), &[object(key)], dry_run(true))
        .await?;
    let record: serde_json::Value = serde_json::from_str(&preview)?;
    let command = record["command"].as_str().unwrap();
    assert_eq!(
        shlex::split(command).unwrap(),
        ["printf", "<%s>", &format!("s3://test-bucket/{key}")]
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn test_exec_target_of_a_version_is_its_url() -> Result<(), Error> {
    let cmd = Cmd::Exec(Exec {
        utility: "echo {}".to_owned(),
    });

    let out = cmd
        .execute_captured(
            &FakeS3Client::default(),
            &test_path(),
            &[version("a.txt", "v1")],
        )
        .await?;

    assert_eq!(out, "s3://test-bucket/a.txt?versionId=v1\n\n");
    Ok(())
}

#[tokio::test]
async fn test_exec_rejects_unusable_utilities() {
    for utility in ["", "  ", "echo 'unclosed {}"] {
        let exec = Exec {
            utility: utility.to_owned(),
        };
        assert!(exec.command("s3://b/k").is_err(), "{utility:?}");
    }
    assert!(exec::run(&[]).await.is_err());
}

fn bucket_path(prefix: Option<&str>) -> S3Path {
    S3Path {
        bucket: "test-bucket".to_owned(),
        prefix: prefix.map(str::to_owned),
    }
}

fn move_to(destination: S3Path, flat: bool) -> Cmd {
    Cmd::Move(S3Move {
        destination,
        flat,
        storage_class: Some(StorageClassName::from("STANDARD_IA")),
    })
}

fn copy_to(destination: S3Path, flat: bool) -> Cmd {
    Cmd::Copy(S3Copy {
        destination,
        flat,
        storage_class: None,
    })
}

/// Source and destination key of each copy, in order.
fn copies(client: &FakeS3Client) -> Vec<(String, String)> {
    let mut copies: Vec<_> = client
        .copy_calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| (call.copy_source.clone(), call.key.clone()))
        .collect();
    copies.sort();
    copies
}

fn deleted(client: &FakeS3Client) -> Vec<String> {
    client
        .delete_calls
        .lock()
        .unwrap()
        .iter()
        .flat_map(|(_, objects)| objects.iter().map(|object| object.key.clone()))
        .collect()
}

#[tokio::test]
async fn test_move_never_deletes_an_object_moved_onto_itself() -> Result<(), Error> {
    let photos = bucket_path(Some("photos"));
    let objects = [object("photos/a.jpg"), object("photos/2020/b.jpg")];

    // Flattening photos/ into itself: photos/a.jpg is already where it would go.
    let client = FakeS3Client::default();
    move_to(photos.clone(), true)
        .execute_captured(&client, &photos, &objects)
        .await?;
    assert_eq!(
        copies(&client),
        [(
            "test-bucket/photos%2F2020%2Fb.jpg".to_owned(),
            "photos/b.jpg".to_owned()
        )]
    );
    assert_eq!(deleted(&client), ["photos/2020/b.jpg"]);

    // Every object is already in place at the root of its own bucket.
    let client = FakeS3Client::default();
    let out = move_to(bucket_path(None), false)
        .execute_captured(&client, &photos, &objects)
        .await?;
    assert_eq!(out, "");
    assert!(copies(&client).is_empty());
    assert!(deleted(&client).is_empty());
    Ok(())
}

#[tokio::test]
async fn test_copy_into_the_searched_path_skips_objects_under_the_destination() -> Result<(), Error>
{
    let client = FakeS3Client::default();
    let objects = [
        object("a.txt"),
        object("backup/a.txt"),
        object("backup/backup/a.txt"),
    ];

    let out = copy_to(bucket_path(Some("backup")), false)
        .execute_captured(&client, &bucket_path(None), &objects)
        .await?;

    assert_eq!(
        out,
        "copying: s3://test-bucket/a.txt => s3://test-bucket/backup/a.txt\n"
    );
    assert_eq!(copies(&client).len(), 1);

    // Copies made outside the searched path aren't listed, so nothing is skipped.
    let client = FakeS3Client::default();
    copy_to(bucket_path(Some("backup")), false)
        .execute_captured(
            &client,
            &bucket_path(Some("backup/old")),
            &[object("backup/old/a.txt")],
        )
        .await?;
    assert_eq!(
        copies(&client),
        [(
            "test-bucket/backup%2Fold%2Fa.txt".to_owned(),
            "backup/backup/old/a.txt".to_owned()
        )]
    );
    Ok(())
}

#[tokio::test]
async fn test_flat_copies_refuse_two_keys_with_one_destination() -> Result<(), Error> {
    let client = FakeS3Client::default();
    let path = test_path();
    let cmd = move_to(
        S3Path {
            bucket: "dest-bucket".to_owned(),
            prefix: Some("flat".to_owned()),
        },
        true,
    );
    let mut out = Vec::new();
    let mut ctx = CommandContext::new(&client, &path, &mut out, CommandOptions::default());

    // Versions of one key may share a destination.
    cmd.execute(
        &mut ctx,
        &[version("a/x.txt", "v2"), version("a/x.txt", "v1")],
    )
    .await?;

    // A different key with the same name is refused, even in a later batch,
    // before anything in its batch is copied or deleted.
    let err = cmd
        .execute(&mut ctx, &[object("b/y.txt"), object("b/x.txt")])
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "s3://test-bucket/a/x.txt and s3://test-bucket/b/x.txt would both be copied to s3://dest-bucket/flat/x.txt"
    );
    assert_eq!(copies(&client).len(), 2);
    assert_eq!(deleted(&client), ["a/x.txt", "a/x.txt"]);
    Ok(())
}

const MIB: i64 = 1024 * 1024;
const GIB: i64 = 1024 * MIB;

fn sized(key: &str, size: i64) -> StreamObject {
    StreamObject::from_object(Object::builder().key(key).size(size).build())
}

fn head(size: i64) -> ObjectHead {
    ObjectHead {
        size,
        e_tag: Some("\"abc\"".to_owned()),
        content_type: Some("video/mp4".to_owned()),
        metadata: [("owner".to_owned(), "data-team".to_owned())].into(),
        ..ObjectHead::default()
    }
}

fn env_prod() -> Vec<Tag> {
    vec![Tag::builder().key("env").value("prod").build().unwrap()]
}

fn dest_bucket(prefix: Option<&str>) -> S3Path {
    S3Path {
        bucket: "dest-bucket".to_owned(),
        prefix: prefix.map(str::to_owned),
    }
}

#[tokio::test]
async fn test_objects_over_5_gib_are_copied_in_parts() -> Result<(), Error> {
    let size = 6 * GIB;
    let client = FakeS3Client::default()
        .with_object_head("test-bucket", "videos/big.mp4", None, head(size))
        .with_tags("test-bucket", "videos/big.mp4", None, env_prod());
    let cmd = Cmd::Copy(S3Copy {
        destination: dest_bucket(Some("archive")),
        flat: false,
        storage_class: Some(StorageClassName::from("GLACIER")),
    });

    let out = cmd
        .execute_captured(&client, &test_path(), &[sized("videos/big.mp4", size)])
        .await?;

    assert_eq!(
        out,
        "copying: s3://test-bucket/videos%2Fbig.mp4 => s3://dest-bucket/archive/videos/big.mp4\n"
    );
    assert!(client.copy_calls.lock().unwrap().is_empty());

    // Metadata and tags carried over, then 12 parts of 512 MiB, each only if
    // the source still has the ETag it had when the copy started.
    let parts = (0..12).map(|i| MultipartCall::Part {
        upload_id: "upload-1".to_owned(),
        part_number: i + 1,
        copy_source: "test-bucket/videos%2Fbig.mp4".to_owned(),
        range: i64::from(i) * 512 * MIB..=i64::from(i + 1) * 512 * MIB - 1,
        source_e_tag: Some("\"abc\"".to_owned()),
    });
    let expected: Vec<_> = [MultipartCall::Create {
        bucket: "dest-bucket".to_owned(),
        key: "archive/videos/big.mp4".to_owned(),
        source: Box::new(head(size)),
        storage_class: Some("GLACIER".to_owned()),
        tags: vec![("env".to_owned(), "prod".to_owned())],
    }]
    .into_iter()
    .chain(parts)
    .chain([MultipartCall::Complete {
        upload_id: "upload-1".to_owned(),
        parts: (1..=12).map(|n| (n, format!("etag-{n}"))).collect(),
    }])
    .collect();
    assert_eq!(*client.multipart_calls.lock().unwrap(), expected);
    Ok(())
}

#[tokio::test]
async fn test_failed_multipart_copy_is_aborted_and_move_deletes_nothing() {
    let size = 6 * GIB;
    let client = FakeS3Client::default()
        .with_object_head("test-bucket", "big.bin", None, head(size))
        .with_failing_part(3);

    let err = move_to(dest_bucket(None), false)
        .execute_captured(&client, &test_path(), &[sized("big.bin", size)])
        .await
        .unwrap_err();

    assert_eq!(
        format!("{err:#}"),
        "s3://test-bucket/big.bin: PreconditionFailed"
    );
    let calls = client.multipart_calls.lock().unwrap();
    assert_eq!(
        calls.last(),
        Some(&MultipartCall::Abort {
            upload_id: "upload-1".to_owned()
        })
    );
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, MultipartCall::Complete { .. }))
    );
    assert!(deleted(&client).is_empty());
}

#[tokio::test]
async fn test_change_storage_of_a_big_object_copies_it_onto_itself_in_parts() -> Result<(), Error> {
    let size = 5 * GIB + 1;
    let client =
        FakeS3Client::default().with_object_head("test-bucket", "big.bin", None, head(size));
    // Tags fetched for tag filtering are reused.
    let mut object = sized("big.bin", size);
    object.tags = Some(env_prod());

    Cmd::ChangeStorage(ChangeStorage {
        storage_class: StorageClassName::from("STANDARD_IA"),
    })
    .execute_captured(&client, &test_path(), &[object])
    .await?;

    let calls = client.multipart_calls.lock().unwrap();
    assert_eq!(
        calls[0],
        MultipartCall::Create {
            bucket: "test-bucket".to_owned(),
            key: "big.bin".to_owned(),
            source: Box::new(head(size)),
            storage_class: Some("STANDARD_IA".to_owned()),
            tags: vec![("env".to_owned(), "prod".to_owned())],
        }
    );
    assert_eq!(calls.len(), 1 + 11 + 1);
    assert!(client.get_tag_calls.lock().unwrap().is_empty());
    assert!(client.copy_calls.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn test_dry_run_of_a_big_copy_makes_no_requests() -> Result<(), Error> {
    let client = FakeS3Client::default();

    copy_to(dest_bucket(None), false)
        .execute_with(
            &client,
            &test_path(),
            &[sized("big.bin", 6 * GIB)],
            dry_run(false),
        )
        .await?;

    assert!(!made_changes(&client));
    Ok(())
}

#[tokio::test]
async fn test_ls_print0_ends_each_url_with_a_nul_byte() -> Result<(), Error> {
    let mut versioned = StreamObject::from_object(Object::builder().key("v.txt").build());
    versioned.version_id = Some("v1".to_owned());
    versioned.is_latest = Some(true);
    let objects = [
        StreamObject::from_object(Object::builder().key("line\nbreak.txt").build()),
        versioned,
    ];
    let cmd = Cmd::Ls(FastPrint { print0: true });
    let client = FakeS3Client::default();

    let out = cmd
        .execute_captured(&client, &test_path(), &objects)
        .await?;
    assert_eq!(
        out,
        "s3://test-bucket/line\nbreak.txt\0s3://test-bucket/v.txt?versionId=v1\0"
    );

    // --json takes precedence.
    let records = json_lines(&cmd, &client, &objects).await;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["key"], "line\nbreak.txt");
    Ok(())
}
