use std::fs;
use std::io::{self, Write};
use std::process::exit;

use clap::CommandFactory;
use clap_complete::aot::{generate, Shell};

include!("src/arg.rs");

fn main() {
    if !version_check::is_min_version("1.31").unwrap_or(false) {
        writeln!(&mut io::stderr(), "This crate requires rustc >= 1.31").unwrap();
        exit(1);
    }

    let var = std::env::var_os("SHELL_COMPLETIONS_DIR").or_else(|| std::env::var_os("OUT_DIR"));
    let outdir = match var {
        None => return,
        Some(outdir) => std::path::PathBuf::from(outdir),
    };
    fs::create_dir_all(&outdir).unwrap();

    let mut cmd = FindOpt::command();
    let name = cmd.get_name().to_string();

    eprintln!("Generating completion file ...");
    generate(
        Shell::Bash,
        &mut cmd,
        &name,
        &mut fs::File::create(outdir.join(format!("{}.bash", &name))).unwrap(),
    );
    generate(
        Shell::Elvish,
        &mut cmd,
        &name,
        &mut fs::File::create(outdir.join(format!("{}.elvish", &name))).unwrap(),
    );
    generate(
        Shell::Fish,
        &mut cmd,
        &name,
        &mut fs::File::create(outdir.join(format!("{}.fish", &name))).unwrap(),
    );
    generate(
        Shell::Zsh,
        &mut cmd,
        &name,
        &mut fs::File::create(outdir.join(format!("{}.zsh", &name))).unwrap(),
    );
    generate(
        Shell::PowerShell,
        &mut cmd,
        &name,
        &mut fs::File::create(outdir.join(format!("{}.ps1", &name))).unwrap(),
    );
}
