use failure::Error;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;
use s3find::run::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let status: Find = FindOpt::from_args().into();

    let stats = list_filter_execute_stream(
        status.into_stream().stream(),
        status.limit,
        status.stats(),
        |x| status.filters.test_match(x),
        &mut |acc, x| status.exec(acc, &x),
    )
    .await;

    if status.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}
