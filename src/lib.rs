#[macro_use]
extern crate structopt;

#[macro_use]
extern crate failure;

extern crate chrono;
extern crate clap;
extern crate futures;
extern crate glob;
extern crate indicatif;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

pub mod arg;
pub mod command;
pub mod credential;
pub mod error;
pub mod filter;
pub mod function;
