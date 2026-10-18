use std::env;
use std::fs::{self, File};
use std::path::PathBuf;

use clap::CommandFactory;
use clap_complete::aot::{Shell, generate};

include!("src/arg.rs");

/// Writes shell completions to `$SHELL_COMPLETIONS_DIR`, or `$OUT_DIR` by default.
fn main() {
    println!("cargo::rerun-if-changed=src/arg.rs");
    println!("cargo::rerun-if-env-changed=SHELL_COMPLETIONS_DIR");

    let Some(outdir) = env::var_os("SHELL_COMPLETIONS_DIR").or_else(|| env::var_os("OUT_DIR"))
    else {
        return;
    };
    let outdir = PathBuf::from(outdir);
    fs::create_dir_all(&outdir).expect("create the completions directory");

    let mut cmd = FindOpt::command();
    let name = cmd.get_name().to_string();

    for (shell, extension) in [
        (Shell::Bash, "bash"),
        (Shell::Elvish, "elvish"),
        (Shell::Fish, "fish"),
        (Shell::Zsh, "zsh"),
        (Shell::PowerShell, "ps1"),
    ] {
        let path = outdir.join(format!("{name}.{extension}"));
        let mut file = File::create(&path).expect("create a completion file");
        generate(shell, &mut cmd, &name, &mut file);
    }
}
