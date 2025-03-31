use aws_sdk_s3::types::Object;
use futures::Future;
use futures::stream::Stream;
use futures::stream::StreamExt;

use crate::command::FindStat;

const CHUNK: usize = 1000;

pub async fn list_filter_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<Object>>,
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
        Some(limit) => list_filter_limit_execute(iterator, limit, stats, p, f).await,
        None => list_filter_unlimited_execute(iterator, stats, p, f).await,
    }
}

#[inline]
async fn list_filter_limit_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<Object>>,
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
        .map(|x| futures::stream::iter(x.into_iter()))
        .flatten()
        .filter(p)
        .take(limit)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}

#[inline]
async fn list_filter_unlimited_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<Object>>,
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
        .map(|x| futures::stream::iter(x.into_iter()))
        .flatten()
        .filter(p)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::Object;
    use futures::stream;
    use std::future::ready;

    #[tokio::test]
    async fn test_list_filter_execute_with_limit() {
        let objects = vec![
            Object::builder().key("object1").build(),
            Object::builder().key("object2").build(),
            Object::builder().key("object3").build(),
        ];

        let iterator = stream::iter(vec![objects]);
        let limit = Some(2);
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &Object| ready(true),
            &mut |acc, list| {
                ready(
                    acc.map(|stat| stat + &list)
                        .or_else(|| Some(FindStat::default() + &list)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_without_limit() {
        let objects = vec![
            Object::builder().key("object1").build(),
            Object::builder().key("object2").build(),
            Object::builder().key("object3").build(),
        ];

        let iterator = stream::iter(vec![objects]);
        let limit = None;
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &Object| ready(true),
            &mut |acc, list| {
                ready(
                    acc.map(|stat| stat + &list)
                        .or_else(|| Some(FindStat::default() + &list)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }

    #[tokio::test]
    async fn test_list_filter_limit_execute() {
        let objects = vec![
            Object::builder().key("object1").build(),
            Object::builder().key("object2").build(),
            Object::builder().key("object3").build(),
        ];

        let iterator = stream::iter(vec![objects]);
        let limit = 2;
        let stats = None;

        let result = list_filter_limit_execute(
            iterator,
            limit,
            stats,
            |_: &Object| ready(true),
            &mut |acc, list| {
                ready(
                    acc.map(|stat| stat + &list)
                        .or_else(|| Some(FindStat::default() + &list)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_unlimited_execute() {
        let objects = vec![
            Object::builder().key("object1").build(),
            Object::builder().key("object2").build(),
            Object::builder().key("object3").build(),
        ];

        let iterator = stream::iter(vec![objects]);
        let stats = None;

        let result = list_filter_unlimited_execute(
            iterator,
            stats,
            |_: &Object| ready(true),
            &mut |acc, list| {
                ready(
                    acc.map(|stat| stat + &list)
                        .or_else(|| Some(FindStat::default() + &list)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }
}
