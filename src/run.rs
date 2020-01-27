use failure::Error;
use itertools::Itertools;
use rusoto_s3::Object;

use crate::command::{FindIter, FindStat};

const CHUNK: usize = 1000;

pub fn list_filter_execute<P, F>(
    iterator: FindIter,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: Fn(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
{
    match limit {
        Some(limit) => list_filter_limit_execute(iterator, limit, stats, p, f),
        None => list_filter_unlimited_execute(iterator, stats, p, f),
    }
}

#[inline]
fn list_filter_limit_execute<P, F>(
    iterator: FindIter,
    limit: usize,
    stats: Option<FindStat>,
    p: P,
    f: F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: Fn(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
{
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(p)
        .take(limit)
        .chunks(CHUNK)
        .into_iter()
        .try_fold(stats, |acc, x| f(acc, &x.collect::<Vec<Object>>()))
}

#[inline]
fn list_filter_unlimited_execute<P, F>(
    iterator: FindIter,
    stats: Option<FindStat>,
    p: P,
    f: F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: Fn(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
{
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(p)
        .chunks(CHUNK)
        .into_iter()
        .try_fold(stats, |acc, x| f(acc, &x.collect::<Vec<Object>>()))
}
