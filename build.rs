use std::fs;
use std::io::{self, Write};
use std::process::exit;
use structopt::clap::Shell;

include!("src/arg.rs");

fn main() {
    if !version_check::is_min_version("1.31").unwrap_or(false) {
        writeln!(&mut io::stderr(), "This crate requires rustc >= 1.31").unwrap();
        exit(1);
    }

    let var = std::env::var_os("SHELL_COMPLETIONS_DIR").or_else(|| std::env::var_os("OUT_DIR"));
    let outdir = match var {
        None => return,
        Some(outdir) => outdir,
    };
    fs::create_dir_all(&outdir).unwrap();

    let mut app = FindOpt::clap();
    app.gen_completions("s3find", Shell::Bash, &outdir);
    app.gen_completions("s3find", Shell::Fish, &outdir);
    app.gen_completions("s3find", Shell::Zsh, &outdir);
    app.gen_completions("s3find", Shell::PowerShell, &outdir);
}
