use failure::Error;
use rusoto_s3::*;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;

fn main() -> Result<(), Error> {
    let status: Find = FindOpt::from_args().into();
    let mut request = status.list_request();
    let mut count: usize = 0;
    let mut stats = status.stats();

    loop {
        match status.limit {
            Some(limit) if limit <= count => {
                break;
            }
            _ => {}
        }

        let output = status.client.list_objects_v2(request.clone()).sync()?;
        match output.contents {
            Some(klist) => {
                let flist: Vec<_> = klist
                    .iter()
                    .filter(|x| status.filters.test_match(x))
                    .collect();
                let len = flist.len();
                count += len;

                let slice = match status.limit {
                    Some(limit) if count.saturating_sub(limit) > 0 => {
                        println!("{},{},{}", len, count, limit);
                        &flist[0..(len - (count - limit))]
                    }
                    _ => &flist,
                };

                stats = status.exec(slice, stats)?;

                match output.next_continuation_token {
                    Some(token) => request.continuation_token = Some(token),
                    None => break,
                }
            }
            None => {
                println!("No keys!");
                break;
            }
        }
    }
    if status.stats {
        println!("{}", stats.unwrap());
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
