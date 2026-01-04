use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Error;

use clap::Parser;
use s3find::adapters::aws;
use s3find::arg::*;
use s3find::command::*;
use s3find::filter::TagFilterList;
use s3find::filter_list::FilterList;
use s3find::run::*;
use s3find::tag_fetcher::{TagFetchConfig, TagFetchStats};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = FindOpt::parse();

    let client = aws::setup_client(&args).await;

    let filters = FilterList::from_opts(&args);
    let tag_filters = TagFilterList::from_opts(&args);
    let command = FindCommand::from_opts(&args, client.clone());
    let stream = FindStream::from_opts(&args, client.clone()).stream();
    let stats = default_stats(args.summarize);

    // Use two-phase filtering if tag filters are configured
    let stats = if tag_filters.has_filters() {
        let tag_config = TagFetchConfig::default().with_concurrency(args.tag_concurrency);
        let tag_stats = Arc::new(TagFetchStats::new());

        // Show warning for large operations
        if args.limit.is_none() {
            eprintln!(
                "Note: Tag filtering requires an API call per object. \
                Consider using --limit to restrict the number of objects processed."
            );
        }

        let result = list_filter_execute_with_tags(
            stream,
            args.limit,
            stats,
            client,
            args.path.bucket.clone(),
            tag_filters,
            tag_config,
            Arc::clone(&tag_stats),
            |x| ready(filters.test_match(x)),
            &mut |acc, x| command.exec(acc, x),
        )
        .await;

        // Print tag fetch statistics if summarize is enabled
        if args.summarize {
            let success = tag_stats.success.load(Ordering::Relaxed);
            let failed = tag_stats.failed.load(Ordering::Relaxed);
            let throttled = tag_stats.throttled.load(Ordering::Relaxed);
            let access_denied = tag_stats.access_denied.load(Ordering::Relaxed);

            if success > 0 || failed > 0 {
                eprintln!(
                    "Tag fetch stats: {} success, {} failed, {} throttled, {} access denied",
                    success, failed, throttled, access_denied
                );
            }
        }

        result
    } else {
        list_filter_execute(
            stream,
            args.limit,
            stats,
            |x| ready(filters.test_match(x)),
            &mut |acc, x| command.exec(acc, x),
        )
        .await
    };

    if args.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}
