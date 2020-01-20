use failure::Error;
use itertools::Itertools;
use rusoto_s3::Object;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;

fn main() -> Result<(), Error> {
    let status: Find = FindOpt::from_args().into();

    let iterator = status.iter().collect::<Result<Vec<Vec<Object>>, Error>>()?;

    let stats = match status.limit {
        Some(limit) => iterator
            .iter()
            .flatten()
            .filter(|x| status.filters.test_match(x))
            .take(limit)
            .chunks(1000)
            .into_iter()
            .fold(status.stats(), |acc, x| {
                status.exec(&x.collect::<Vec<_>>(), acc).unwrap()
            }),
        None => iterator
            .iter()
            .flatten()
            .filter(|x| status.filters.test_match(x))
            .chunks(1000)
            .into_iter()
            .fold(status.stats(), |acc, x| {
                status.exec(&x.collect::<Vec<_>>(), acc).unwrap()
            }),
    };

    if status.summarize {
        println!("{}", stats.unwrap());
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
