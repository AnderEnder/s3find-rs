use failure::Error;
use itertools::Itertools;
use rusoto_s3::Object;

use crate::command::{FilterList, Find, FindIter, FindStat};

const CHUNK: usize = 1000;

pub fn list_filter_execute(
    iterator: FindIter,
    limit: Option<usize>,
    find: &Find,
) -> Result<Option<FindStat>, Error> {
    match limit {
        Some(limit) => list_filter_limit_execute(iterator, limit, find),
        None => list_filter_unlimited_execute(iterator, find),
    }
}

fn list_filter_limit_execute(
    iterator: FindIter,
    limit: usize,
    find: &Find,
) -> Result<Option<FindStat>, Error> {
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(|x| find.filters.test_match(x))
        .take(limit)
        .chunks(CHUNK)
        .into_iter()
        .try_fold(find.stats(), |acc, x| {
            find.exec(&x.collect::<Vec<Object>>(), acc)
        })
}

fn list_filter_unlimited_execute(
    iterator: FindIter,
    find: &Find,
) -> Result<Option<FindStat>, Error> {
    iterator
        .map(|x| x.unwrap())
        .flatten()
        .filter(|x| find.filters.test_match(x))
        .chunks(CHUNK)
        .into_iter()
        .try_fold(find.stats(), |acc, x| {
            find.exec(&x.collect::<Vec<Object>>(), acc)
        })
}
