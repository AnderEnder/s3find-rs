use anyhow::Error;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;
use s3find::run::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = FindOpt::from_args();
    let (find, filters) = Find::from_opts(&args).await;

    let stats = list_filter_execute(
        find.to_stream().stream(),
        find.limit,
        default_stats(find.summarize),
        |x| filters.test_match(x.clone()),
        &mut |acc, x| find.exec(acc, x),
    )
    .await;

    if find.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}
