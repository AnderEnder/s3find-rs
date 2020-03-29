use failure::Error;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::Future;

use itertools::Itertools;
use rusoto_s3::Object;

use crate::command::{FindIter, FindStat};

const CHUNK: usize = 1000;

pub fn list_filter_execute<P, F>(
    iterator: FindIter,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: FnMut(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
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
    f: &mut F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: FnMut(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
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
    f: &mut F,
) -> Result<Option<FindStat>, Error>
where
    P: Fn(&Object) -> bool,
    F: FnMut(Option<FindStat>, &[Object]) -> Result<Option<FindStat>, Error>,
{
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(p)
        .chunks(CHUNK)
        .into_iter()
        .try_fold(stats, |acc, x| f(acc, &x.collect::<Vec<Object>>()))
}

pub async fn list_filter_execute_stream<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Result<impl Stream<Item = Object>, Error>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&Object) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<Object>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    match limit {
        Some(limit) => list_filter_limit_execute_stream(iterator, limit, stats, p, f).await,
        None => list_filter_unlimited_execute_stream(iterator, stats, p, f).await,
    }
}

#[inline]
async fn list_filter_limit_execute_stream<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Result<impl Stream<Item = Object>, Error>>,
    limit: usize,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&Object) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<Object>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(p)
        .take(limit)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}

#[inline]
async fn list_filter_unlimited_execute_stream<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Result<impl Stream<Item = Object>, Error>>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&Object) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<Object>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(p)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}
