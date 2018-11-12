use failure::Error;
use rusoto_s3::*;
use structopt::StructOpt;

use s3find::arg::*;
use s3find::command::*;

fn main() -> Result<(), Error> {
    let status: FindCommand = FindOpt::from_args().into();
    let mut request = status.list_request();
    let mut count: usize = 0;

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
                    Some(limit) if (count - limit) > 0 => &flist[0..(len - count + limit)],
                    _ => &flist,
                };

                status.exec(slice)?;

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
    Ok(())
}

#[cfg(test)]
mod tests {}
