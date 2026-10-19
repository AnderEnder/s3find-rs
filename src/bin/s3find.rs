use std::io::{self, BufWriter, Write};
use std::pin::pin;
use std::process::ExitCode;

use clap::Parser;
use s3find::adapters::aws;
use s3find::arg::FindOpt;
use s3find::error::{S3FindError, S3FindResult};
use s3find::pipeline::{Pipeline, option_warnings};
use s3find::stats::Summary;
use serde_json::json;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> ExitCode {
    let args = FindOpt::parse();

    let Err(err) = run_cli(&args).await else {
        return ExitCode::SUCCESS;
    };
    if args.json {
        eprintln!("{}", json!({ "error": err.to_string() }));
    } else {
        eprintln!("{err}");
    }
    match err {
        // 128 + SIGINT, as shells report a command stopped by Ctrl-C.
        S3FindError::Interrupted => ExitCode::from(130),
        _ => ExitCode::FAILURE,
    }
}

async fn run_cli(args: &FindOpt) -> S3FindResult<()> {
    for warning in option_warnings(args) {
        eprintln!("{warning}");
    }

    let mut client = aws::setup_client(args).await;
    if aws::looks_up_bucket_region(args) {
        client = aws::in_bucket_region(client, &args.path.bucket).await;
    }
    let pipeline = Pipeline::from_opts(args, client);

    let mut out: Box<dyn Write + Send> = if pipeline.buffered_output() {
        Box::new(BufWriter::new(io::stdout()))
    } else {
        Box::new(io::stdout())
    };
    // The first Ctrl-C lets requests in flight finish but starts no new ones;
    // a second stops at once, dropping them.
    let mut run = pin!(pipeline.run(&mut out));
    let stats = tokio::select! {
        result = &mut run => result?,
        _ = tokio::signal::ctrl_c() => {
            pipeline.interrupt();
            eprintln!("Interrupted: finishing requests in flight, press Ctrl-C again to stop now");
            tokio::select! {
                result = &mut run => result?,
                _ = tokio::signal::ctrl_c() => return Err(S3FindError::Interrupted),
            }
        }
    };
    if pipeline.interrupted() {
        return Err(S3FindError::Interrupted);
    }

    if let Some(stats) = stats {
        let tag_fetch = pipeline.tag_stats().filter(|stats| !stats.is_empty());
        if args.json {
            let summary = Summary {
                stats: &stats,
                tag_fetch,
            };
            println!("{}", json!({ "summary": summary }));
        } else {
            if let Some(tag_fetch) = tag_fetch {
                eprintln!("Tag fetch stats: {tag_fetch}");
            }
            println!("{stats}");
        }
    }

    Ok(())
}
