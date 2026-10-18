use std::io::{self, BufWriter, Write};
use std::process::ExitCode;

use clap::Parser;
use s3find::adapters::aws;
use s3find::arg::FindOpt;
use s3find::error::S3FindResult;
use s3find::pipeline::{Pipeline, option_warnings};

#[tokio::main]
async fn main() -> ExitCode {
    match run_cli().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

async fn run_cli() -> S3FindResult<()> {
    let args = FindOpt::parse();

    for warning in option_warnings(&args) {
        eprintln!("{warning}");
    }

    let client = aws::setup_client(&args).await;
    let pipeline = Pipeline::from_opts(&args, client);

    let mut out: Box<dyn Write + Send> = if pipeline.buffered_output() {
        Box::new(BufWriter::new(io::stdout()))
    } else {
        Box::new(io::stdout())
    };
    let stats = pipeline.run(&mut out).await?;

    if args.summarize {
        if let Some(tag_stats) = pipeline.tag_stats().filter(|stats| !stats.is_empty()) {
            eprintln!("Tag fetch stats: {tag_stats}");
        }
        if let Some(stats) = stats {
            println!("{stats}");
        }
    }

    Ok(())
}
