#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use anyhow::Error;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;
use s3find::run::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let status: Find = FindOpt::from_args().into();
    let filters = status.filters.clone();

    let stats = list_filter_execute(
        status.to_stream().stream(),
        status.limit,
        default_stats(status.summarize),
        |x| filters.test_match(x.clone()),
        &mut |acc, x| status.exec(acc, x),
    )
    .await;

    if status.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}
