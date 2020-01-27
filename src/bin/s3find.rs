use failure::Error;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;
use s3find::run::list_filter_execute;

fn main() -> Result<(), Error> {
    let status: Find = FindOpt::from_args().into();

    let stats = list_filter_execute(
        status.iter(),
        status.limit,
        status.stats(),
        |x| status.filters.test_match(x),
        |acc, x| status.exec(acc, x),
    )?;

    if status.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
