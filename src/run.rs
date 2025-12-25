use futures::Future;
use futures::stream::Stream;
use futures::stream::StreamExt;

use crate::command::{FindStat, StreamObject};

const CHUNK: usize = 1000;

pub async fn list_filter_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<StreamObject>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    match limit {
        Some(limit) => list_filter_limit_execute(iterator, limit, stats, p, f).await,
        None => list_filter_unlimited_execute(iterator, stats, p, f).await,
    }
}

#[inline]
async fn list_filter_limit_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<StreamObject>>,
    limit: usize,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
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
    iterator: impl Stream<Item = Vec<StreamObject>>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
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

    fn make_stream_objects(keys: &[&str]) -> Vec<StreamObject> {
        keys.iter()
            .map(|k| StreamObject::from_object(Object::builder().key(*k).build()))
            .collect()
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_limit() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = Some(2);
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_without_limit() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = None;
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }

    #[tokio::test]
    async fn test_list_filter_limit_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = 2;
        let stats = None;

        let result = list_filter_limit_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_unlimited_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let stats = None;

        let result = list_filter_unlimited_execute(
            iterator,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }
}
