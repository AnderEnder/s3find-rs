use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use aws_sdk_s3::types::{Object, Tag};
use futures::stream;

use super::*;
use crate::arg::{TagExistsFilter, TagFilter};
use crate::filter::Filter;
use crate::testing::{FakeS3Client, opts};

/// Records the keys of every batch it is given.
#[derive(Clone, Default)]
struct Recorder {
    batches: Arc<Mutex<Vec<Vec<String>>>>,
    fail: bool,
}

impl Recorder {
    fn failing() -> Self {
        Self {
            fail: true,
            ..Self::default()
        }
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batches.lock().unwrap().iter().map(Vec::len).collect()
    }

    fn keys(&self) -> Vec<String> {
        self.batches.lock().unwrap().concat()
    }
}

impl RunCommand for Recorder {
    async fn execute<C: S3Client>(
        &self,
        _: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), anyhow::Error> {
        if self.fail {
            return Err(anyhow!("command failed"));
        }
        self.batches.lock().unwrap().push(
            objects
                .iter()
                .map(|object| object.key().unwrap_or_default().to_string())
                .collect(),
        );
        Ok(())
    }
}

/// Passes everything and counts how often it was asked.
#[derive(Clone, Default)]
struct CountingFilter(Arc<AtomicUsize>);

impl CountingFilter {
    fn calls(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Filter for CountingFilter {
    fn filter(&self, _: &StreamObject) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed);
        true
    }
}

struct RejectAll;

impl Filter for RejectAll {
    fn filter(&self, _: &StreamObject) -> bool {
        false
    }
}

fn listing() -> ListingOptions {
    ListingOptions::from_opts(&opts(&["s3://test-bucket"]))
}

fn pipeline(client: FakeS3Client, recorder: &Recorder) -> Pipeline<FakeS3Client, Recorder> {
    Pipeline::new(client, listing(), recorder.clone())
}

fn keyed(key: &str) -> StreamObject {
    StreamObject::from_object(Object::builder().key(key).size(1).build())
}

fn tags(pairs: &[(&str, &str)]) -> Vec<Tag> {
    pairs
        .iter()
        .map(|(k, v)| Tag::builder().key(*k).value(*v).build().unwrap())
        .collect()
}

fn tagged(key: &str, pairs: &[(&str, &str)]) -> StreamObject {
    let mut object = keyed(key);
    object.tags = Some(tags(pairs));
    object
}

fn objects(
    items: impl IntoIterator<Item = StreamObject>,
) -> impl Stream<Item = S3FindResult<StreamObject>> + Send {
    stream::iter(items.into_iter().map(Ok).collect::<Vec<_>>())
}

fn env_prod() -> FilterList {
    FilterList::new().with(TagFilter {
        key: "env".to_string(),
        value: "prod".to_string(),
    })
}

async fn process(
    pipeline: &Pipeline<FakeS3Client, impl RunCommand>,
    items: impl IntoIterator<Item = StreamObject>,
) -> S3FindResult<Option<FindStat>> {
    pipeline.process(objects(items), &mut Vec::new()).await
}

#[tokio::test]
async fn test_runs_command_on_all_matches() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder);

    process(&pipeline, ["a", "b", "c"].map(keyed))
        .await
        .unwrap();

    assert_eq!(recorder.keys(), ["a", "b", "c"]);
}

#[tokio::test]
async fn test_skips_non_matching_objects() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder)
        .with_filters(FilterList::new().with(RejectAll));

    process(&pipeline, ["a", "b"].map(keyed)).await.unwrap();

    assert!(recorder.batches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_limit_stops_filtering_early() {
    let recorder = Recorder::default();
    let counter = CountingFilter::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder)
        .with_filters(FilterList::new().with(counter.clone()))
        .with_limit(Some(2));

    process(&pipeline, ["a", "b", "c"].map(keyed))
        .await
        .unwrap();

    assert_eq!(recorder.keys(), ["a", "b"]);
    assert_eq!(counter.calls(), 2);
}

#[tokio::test]
async fn test_zero_limit_does_no_work() {
    let recorder = Recorder::default();
    let counter = CountingFilter::default();
    let client = FakeS3Client::default().with_objects(&[("a", 1)]);
    let pipeline = Pipeline::new(client, listing(), recorder.clone())
        .with_filters(FilterList::new().with(counter.clone()))
        .with_limit(Some(0));

    pipeline.run(&mut Vec::new()).await.unwrap();

    assert!(recorder.batches.lock().unwrap().is_empty());
    assert_eq!(counter.calls(), 0);
    assert!(pipeline.client().list_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_limit_stops_listing_early() {
    let client = FakeS3Client::default().with_objects(&[("a", 1), ("b", 1), ("c", 1), ("d", 1)]);
    let recorder = Recorder::default();
    let listing = ListingOptions {
        page_size: 1,
        ..listing()
    };
    let pipeline = Pipeline::new(client, listing, recorder.clone()).with_limit(Some(2));

    pipeline.run(&mut Vec::new()).await.unwrap();

    assert_eq!(recorder.keys(), ["a", "b"]);
    assert_eq!(pipeline.client().list_calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn test_batches_are_capped() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder);

    let items = (0..=BATCH_SIZE).map(|i| keyed(&format!("object{i}")));
    process(&pipeline, items).await.unwrap();

    assert_eq!(recorder.batch_sizes(), [BATCH_SIZE, 1]);
}

#[tokio::test]
async fn test_summary_counts_executed_objects() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder)
        .with_limit(Some(2))
        .with_summary(true);

    let stats = process(&pipeline, ["a", "b", "c"].map(keyed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(stats.total_files, 2);
    assert_eq!(stats.total_space, 2);
}

#[tokio::test]
async fn test_summary_disabled_returns_none() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder);

    assert!(process(&pipeline, [keyed("a")]).await.unwrap().is_none());
}

#[tokio::test]
async fn test_summary_without_matches_is_empty() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder).with_summary(true);

    let stats = process(&pipeline, []).await.unwrap().unwrap();

    assert_eq!(stats, FindStat::default());
}

#[tokio::test]
async fn test_listing_errors_propagate() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default().with_list_error("boom"), &recorder);

    let result = pipeline.run(&mut Vec::new()).await;

    assert!(matches!(result, Err(S3FindError::ListObjects { .. })));
}

#[tokio::test]
async fn test_command_errors_propagate() {
    let pipeline = pipeline(FakeS3Client::default(), &Recorder::failing());

    let result = process(&pipeline, [keyed("a")]).await;

    assert!(matches!(result, Err(S3FindError::CommandExecution { .. })));
}

/// Notes how many objects had been listed whenever a batch finishes, after
/// letting the rest of the pipeline run.
#[derive(Clone, Default)]
struct ListedDuringExecution {
    listed: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<usize>>>,
}

impl RunCommand for ListedDuringExecution {
    async fn execute<C: S3Client>(
        &self,
        _: &mut CommandContext<'_, C>,
        _: &[StreamObject],
    ) -> Result<(), anyhow::Error> {
        tokio::task::yield_now().await;
        self.observed
            .lock()
            .unwrap()
            .push(self.listed.load(Ordering::Relaxed));
        Ok(())
    }
}

#[tokio::test]
async fn test_listing_runs_ahead_while_a_batch_executes() {
    let command = ListedDuringExecution::default();
    let pipeline = Pipeline::new(FakeS3Client::default(), listing(), command.clone());
    let listed = Arc::clone(&command.listed);
    let items = stream::iter((0..3 * BATCH_SIZE).map(move |i| {
        listed.fetch_add(1, Ordering::Relaxed);
        Ok(keyed(&format!("object{i}")))
    }));

    pipeline.process(items, &mut Vec::new()).await.unwrap();

    let observed = command.observed.lock().unwrap();
    assert_eq!(observed.len(), 3);
    assert!(observed[0] > BATCH_SIZE, "listed {observed:?}");
}

#[tokio::test]
async fn test_command_errors_stop_listing() {
    let pipeline = pipeline(FakeS3Client::default(), &Recorder::failing());
    let endless = stream::iter((0..).map(|i| Ok(keyed(&format!("object{i}")))));

    let result = pipeline.process(endless, &mut Vec::new()).await;

    assert!(matches!(result, Err(S3FindError::CommandExecution { .. })));
}

#[test]
fn test_from_opts_sets_command_options() {
    let options =
        |args: &[&str]| Pipeline::from_opts(&opts(args), FakeS3Client::default()).command_options;

    assert_eq!(options(&["s3://bucket"]), CommandOptions::default());
    assert_eq!(
        options(&[
            "s3://bucket",
            "--concurrency",
            "7",
            "--json",
            "delete",
            "--dry-run"
        ]),
        CommandOptions {
            concurrency: 7,
            json: true,
            dry_run: true,
        }
    );
}

#[tokio::test]
async fn test_tag_filters_with_cached_tags() {
    let recorder = Recorder::default();
    let client = FakeS3Client::default();
    let pipeline = pipeline(client, &recorder).with_tag_filters(env_prod(), 2);

    process(
        &pipeline,
        [
            tagged("file1.txt", &[("env", "prod")]),
            tagged("file2.txt", &[("env", "dev")]),
            tagged("file3.txt", &[("env", "prod")]),
        ],
    )
    .await
    .unwrap();

    assert_eq!(recorder.keys(), ["file1.txt", "file3.txt"]);
    assert!(pipeline.tag_stats().unwrap().is_empty());
}

#[tokio::test]
async fn test_tag_filters_fetch_tags() {
    let recorder = Recorder::default();
    let client = FakeS3Client::default()
        .with_tags("test-bucket", "file1.txt", None, tags(&[("env", "prod")]))
        .with_tags("test-bucket", "file2.txt", None, tags(&[("env", "dev")]));
    let pipeline = pipeline(client, &recorder).with_tag_filters(env_prod(), 1);

    process(&pipeline, ["file1.txt", "file2.txt"].map(keyed))
        .await
        .unwrap();

    assert_eq!(recorder.keys(), ["file1.txt"]);
    let stats = pipeline.tag_stats().unwrap();
    assert_eq!(stats.success.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_tags_are_only_fetched_for_cheap_filter_matches() {
    let recorder = Recorder::default();
    let pipeline = Pipeline::new(FakeS3Client::default(), listing(), recorder.clone())
        .with_filters(FilterList::new().with(crate::arg::FindSize::Bigger(5)))
        .with_tag_filters(env_prod(), 4);

    let small = keyed("small.txt");
    let big = StreamObject::from_object(Object::builder().key("big.txt").size(10).build());
    process(&pipeline, [small, big]).await.unwrap();

    let fetched: Vec<_> = pipeline
        .client()
        .get_tag_calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.key.clone())
        .collect();
    assert_eq!(fetched, ["big.txt"]);
}

#[tokio::test]
async fn test_tag_filters_with_limit_keep_traversal_order() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder)
        .with_tag_filters(env_prod(), 3)
        .with_limit(Some(2));

    process(
        &pipeline,
        ["file1.txt", "file2.txt", "file3.txt"].map(|key| tagged(key, &[("env", "prod")])),
    )
    .await
    .unwrap();

    assert_eq!(recorder.keys(), ["file1.txt", "file2.txt"]);
}

#[tokio::test]
async fn test_tag_filters_with_large_input_and_limit() {
    let recorder = Recorder::default();
    let pipeline = pipeline(FakeS3Client::default(), &recorder)
        .with_tag_filters(env_prod(), 50)
        .with_limit(Some(50));

    let items = (0..150).map(|i| tagged(&format!("file{i}.txt"), &[("env", "prod")]));
    process(&pipeline, items).await.unwrap();

    assert_eq!(recorder.keys().len(), 50);
}

#[tokio::test]
async fn test_tag_exists_filter() {
    let recorder = Recorder::default();
    let filters = FilterList::new().with(TagExistsFilter {
        key: "owner".to_string(),
    });
    let pipeline = pipeline(FakeS3Client::default(), &recorder).with_tag_filters(filters, 1);

    process(
        &pipeline,
        [
            tagged("file1.txt", &[("owner", "team-a")]),
            tagged("file2.txt", &[]),
        ],
    )
    .await
    .unwrap();

    assert_eq!(recorder.keys(), ["file1.txt"]);
}

#[tokio::test]
async fn test_empty_tag_filters_skip_tag_stage() {
    let recorder = Recorder::default();
    let pipeline =
        pipeline(FakeS3Client::default(), &recorder).with_tag_filters(FilterList::new(), 10);

    process(&pipeline, [keyed("a")]).await.unwrap();

    assert!(pipeline.tag_stats().is_none());
    assert_eq!(recorder.keys(), ["a"]);
}

fn listed_bucket() -> FakeS3Client {
    FakeS3Client::default().with_objects(&[
        ("logs/app.log", 300),
        ("logs/app.txt", 10),
        ("logs/old/app.log", 200),
        ("readme.txt", 20),
    ])
}

async fn run_cli(client: FakeS3Client, args: &[&str]) -> (String, Option<FindStat>) {
    let pipeline = Pipeline::from_opts(&opts(args), client);
    let mut out = Vec::new();
    let stats = pipeline.run(&mut out).await.unwrap();
    (String::from_utf8(out).unwrap(), stats)
}

#[tokio::test]
async fn test_from_opts_defaults_to_ls() {
    let (out, stats) = run_cli(listed_bucket(), &["s3://test-bucket/logs/"]).await;

    assert_eq!(
        out,
        "s3://test-bucket/logs/app.log\n\
         s3://test-bucket/logs/app.txt\n\
         s3://test-bucket/logs/old/app.log\n"
    );
    assert!(stats.is_none());
}

#[tokio::test]
async fn test_from_opts_applies_filters_limit_and_summary() {
    let (out, stats) = run_cli(
        listed_bucket(),
        &[
            "s3://test-bucket",
            "--name",
            "*.log",
            "--bytes-size",
            "+100",
            "--limit",
            "1",
            "--summarize",
            "ls",
        ],
    )
    .await;

    assert_eq!(out, "s3://test-bucket/logs/app.log\n");
    let stats = stats.unwrap();
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.max_key, "logs/app.log");
}

#[tokio::test]
async fn test_from_opts_with_maxdepth() {
    let (out, _) = run_cli(listed_bucket(), &["s3://test-bucket", "--maxdepth", "0"]).await;

    assert_eq!(out, "s3://test-bucket/readme.txt\n");
}

#[tokio::test]
async fn test_from_opts_filters_on_tags() {
    let client = listed_bucket()
        .with_tags(
            "test-bucket",
            "logs/app.log",
            None,
            tags(&[("env", "prod")]),
        )
        .with_tags("test-bucket", "readme.txt", None, tags(&[("env", "dev")]));

    let pipeline = Pipeline::from_opts(
        &opts(&["s3://test-bucket", "--tag", "env=prod", "lstags"]),
        client,
    );
    let mut out = Vec::new();
    pipeline.run(&mut out).await.unwrap();

    // lstags reuses the tags fetched for filtering.
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "s3://test-bucket/logs/app.log env:prod\n"
    );
    let stats = pipeline.tag_stats().unwrap();
    assert_eq!(stats.success.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn test_from_opts_runs_side_effect_commands() {
    let pipeline = Pipeline::from_opts(
        &opts(&["s3://test-bucket/logs/old", "delete"]),
        listed_bucket(),
    );

    let mut out = Vec::new();
    pipeline.run(&mut out).await.unwrap();

    assert_eq!(
        String::from_utf8(out).unwrap(),
        "deleted: s3://test-bucket/logs/old/app.log\n"
    );
    assert_eq!(pipeline.client().delete_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_buffered_output_follows_command() {
    let client = FakeS3Client::default;

    assert!(Pipeline::from_opts(&opts(&["s3://bkt", "ls"]), client()).buffered_output());
    assert!(Pipeline::from_opts(&opts(&["s3://bkt", "print"]), client()).buffered_output());
    assert!(!Pipeline::from_opts(&opts(&["s3://bkt", "delete"]), client()).buffered_output());
    assert!(
        !Pipeline::from_opts(&opts(&["s3://bkt", "download", "/tmp/x"]), client())
            .buffered_output()
    );
}

#[test]
fn test_option_warnings() {
    assert!(option_warnings(&opts(&["s3://bkt"])).is_empty());
    assert!(option_warnings(&opts(&["s3://bkt", "--tag", "a=b", "--limit", "5"])).is_empty());

    let warnings = option_warnings(&opts(&["s3://bkt", "--tag-exists", "a"]));
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("--limit"));

    let warnings = option_warnings(&opts(&["s3://bkt", "--all-versions", "--maxdepth", "1"]));
    assert_eq!(
        warnings,
        ["Warning: --maxdepth is ignored when --all-versions is used"]
    );
}

#[test]
fn test_option_warnings_note_destinations_inside_the_searched_path() {
    let nested_note = "Note: the destination is inside the searched path; \
        objects already under it are skipped";
    for args in [
        ["s3://bkt", "copy", "s3://bkt/backup"],
        ["s3://bkt/data", "move", "s3://bkt/data/archive"],
    ] {
        assert_eq!(option_warnings(&opts(&args)), [nested_note], "{args:?}");
    }

    for args in [
        &["s3://bkt/data", "copy", "s3://bkt/archive"][..],
        &["s3://bkt", "copy", "s3://other/backup"],
        &["s3://bkt", "move", "--flat", "s3://bkt/backup"],
        &["s3://bkt", "ls"],
    ] {
        assert!(option_warnings(&opts(args)).is_empty(), "{args:?}");
    }
}

#[tokio::test]
async fn test_flat_copy_collisions_are_caught_across_batches() {
    let command = opts(&["s3://test-bucket", "copy", "--flat", "s3://dest-bucket"])
        .cmd
        .unwrap();
    let pipeline = Pipeline::new(FakeS3Client::default(), listing(), command);
    let first_batch = (0..BATCH_SIZE).map(|i| keyed(&format!("a/{i:04}.txt")));

    let err = process(&pipeline, first_batch.chain([keyed("b/0000.txt")]))
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("s3://test-bucket/a/0000.txt and s3://test-bucket/b/0000.txt"),
        "{err}"
    );
    assert_eq!(pipeline.client.copy_calls.lock().unwrap().len(), BATCH_SIZE);
}

/// Pins the examples in the `--maxdepth` help text.
#[tokio::test]
async fn test_maxdepth_help_examples() {
    let bucket = || {
        FakeS3Client::default().with_objects(&[
            ("root.txt", 1),
            ("logs/file.txt", 1),
            ("logs/2024/file.txt", 1),
            ("logs/2024/01/file.txt", 1),
        ])
    };

    let (out, _) = run_cli(bucket(), &["s3://test-bucket/logs/", "--maxdepth", "2"]).await;
    assert_eq!(
        out,
        "s3://test-bucket/logs/file.txt\ns3://test-bucket/logs/2024/file.txt\n"
    );

    let (out, _) = run_cli(bucket(), &["s3://test-bucket", "--maxdepth", "1"]).await;
    assert_eq!(
        out,
        "s3://test-bucket/root.txt\ns3://test-bucket/logs/file.txt\n"
    );
}

#[tokio::test]
async fn test_trailing_slash_limits_the_path_to_its_directory() {
    let bucket = || {
        FakeS3Client::default().with_objects(&[
            ("logs.txt", 1),
            ("logs/file.txt", 1),
            ("logs/2024/file.txt", 1),
            ("logs-old/file.txt", 1),
        ])
    };
    let listed = async |args: &[&str]| run_cli(bucket(), args).await.0;

    assert_eq!(
        listed(&["s3://test-bucket/logs/"]).await,
        "s3://test-bucket/logs/2024/file.txt\ns3://test-bucket/logs/file.txt\n"
    );
    // Without the slash, the path is a plain S3 prefix.
    assert_eq!(
        listed(&["s3://test-bucket/logs"]).await,
        "s3://test-bucket/logs-old/file.txt\n\
         s3://test-bucket/logs.txt\n\
         s3://test-bucket/logs/2024/file.txt\n\
         s3://test-bucket/logs/file.txt\n"
    );

    // Objects directly inside logs/ are at depth 1.
    assert_eq!(
        listed(&["s3://test-bucket/logs/", "--maxdepth", "1"]).await,
        "s3://test-bucket/logs/file.txt\n"
    );
    assert_eq!(
        listed(&["s3://test-bucket/logs/", "--maxdepth", "0"]).await,
        ""
    );
}

#[test]
fn test_run_future_is_send() {
    fn assert_send(_: impl Send) {}

    let pipeline = Pipeline::from_opts(&opts(&["s3://bucket"]), FakeS3Client::default());
    let mut out = Vec::new();
    assert_send(pipeline.run(&mut out));
}
