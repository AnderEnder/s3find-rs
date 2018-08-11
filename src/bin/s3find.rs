extern crate rusoto_s3;
extern crate s3find;
extern crate structopt;

use rusoto_s3::*;
use structopt::StructOpt;

use s3find::commands::*;
use s3find::opts::*;

fn main() -> Result<()> {
    let status_opts = FindOpt::from_args();

    let status: FindCommand = status_opts.clone().into();
    let mut request = status.list_request();

    loop {
        let output = status.client.list_objects_v2(request.clone()).sync()?;
        match output.contents {
            Some(klist) => {
                let flist: Vec<_> = klist.iter().filter(|x| status.filters.filters(x)).collect();
                status.exec(&flist)?;

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
