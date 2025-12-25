use anyhow::Error;

use clap::Parser;
use s3find::adapters::aws;
use s3find::arg::*;
use s3find::command::*;
use s3find::filter_list::FilterList;
use s3find::run::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = FindOpt::parse();

    let client = aws::setup_client(&args).await;

    let filters = FilterList::from_opts(&args);
    let command = FindCommand::from_opts(&args, client.clone());
    let stream = FindStream::from_opts(&args, client).stream();
    let stats = default_stats(args.summarize);

    let stats = list_filter_execute(
        stream,
        args.limit,
        stats,
        |x| filters.test_match(x.clone()),
        &mut |acc, x| command.exec(acc, x),
    )
    .await;

    if args.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}
