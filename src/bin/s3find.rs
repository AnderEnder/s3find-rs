use std::io::{self, BufWriter, Write};
use std::process::ExitCode;

use clap::Parser;
use s3find::adapters::aws;
use s3find::arg::FindOpt;
use s3find::error::S3FindResult;
use s3find::pipeline::{Pipeline, option_warnings};
use s3find::stats::Summary;
use serde_json::json;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> ExitCode {
    let args = FindOpt::parse();

    match run_cli(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) if args.json => {
            eprintln!("{}", json!({ "error": err.to_string() }));
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

async fn run_cli(args: &FindOpt) -> S3FindResult<()> {
    for warning in option_warnings(args) {
        eprintln!("{warning}");
    }

    let client = aws::setup_client(args).await;
    let pipeline = Pipeline::from_opts(args, client);

    let mut out: Box<dyn Write + Send> = if pipeline.buffered_output() {
        Box::new(BufWriter::new(io::stdout()))
    } else {
        Box::new(io::stdout())
    };
    let stats = pipeline.run(&mut out).await?;

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
