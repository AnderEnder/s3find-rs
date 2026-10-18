//! The find pipeline: list → filter → fetch tags (only when filtering on tags)
//! → limit → batch → run the command, collecting summary statistics.

use std::future::ready;
use std::io::Write;

use futures::channel::mpsc;
use futures::future::try_join;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::adapters::aws::S3Client;
use crate::arg::{Cmd, FindOpt};
use crate::error::{S3FindError, S3FindResult};
use crate::filter::FilterList;
use crate::listing::{ListingOptions, list_objects};
use crate::object::StreamObject;
use crate::run_command::{CommandContext, DEFAULT_CONCURRENCY, RunCommand};
use crate::stats::FindStat;
use crate::tag_fetcher::{TagFetchStats, TagFetcher};

/// Most objects handed to a command at once; DeleteObjects accepts up to 1000 keys.
pub const BATCH_SIZE: usize = 1000;

/// Tag filtering: fetch each candidate's tags, then filter on them.
struct TagStage {
    fetcher: TagFetcher,
    filters: FilterList,
}

/// Runs a command over the objects under an S3 path.
///
/// Generic over the S3 client and the command, so the CLI runs
/// `Pipeline<aws_sdk_s3::Client, Cmd>` while tests plug in fakes.
pub struct Pipeline<C, K = Cmd> {
    client: C,
    listing: ListingOptions,
    filters: FilterList,
    tag_stage: Option<TagStage>,
    limit: Option<usize>,
    command: K,
    concurrency: usize,
    summarize: bool,
}

impl<C: S3Client> Pipeline<C> {
    pub fn from_opts(opts: &FindOpt, client: C) -> Self {
        let command = opts.cmd.clone().unwrap_or_default();

        Self::new(client, ListingOptions::from_opts(opts), command)
            .with_filters(FilterList::from_opts(opts))
            .with_tag_filters(
                FilterList::tag_filters_from_opts(opts),
                opts.tag_concurrency.into(),
            )
            .with_limit(opts.limit)
            .with_concurrency(opts.concurrency.into())
            .with_summary(opts.summarize)
    }
}

impl<C: S3Client, K: RunCommand> Pipeline<C, K> {
    /// A pipeline that runs `command` on every listed object.
    pub fn new(client: C, listing: ListingOptions, command: K) -> Self {
        Self {
            client,
            listing,
            filters: FilterList::new(),
            tag_stage: None,
            limit: None,
            command,
            concurrency: DEFAULT_CONCURRENCY,
            summarize: false,
        }
    }

    /// Filters answerable from listing metadata.
    pub fn with_filters(mut self, filters: FilterList) -> Self {
        self.filters = filters;
        self
    }

    /// Filters on object tags, fetched with up to `concurrency` requests in
    /// flight. Tags are only fetched for objects that pass the other filters.
    pub fn with_tag_filters(mut self, filters: FilterList, concurrency: usize) -> Self {
        self.tag_stage = (!filters.is_empty()).then(|| TagStage {
            fetcher: TagFetcher::new(&self.listing.path.bucket, concurrency),
            filters,
        });
        self
    }

    /// Stops after this many matches, in traversal order.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Most requests a command keeps in flight when it makes one per object.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Collects summary statistics over matched objects.
    pub fn with_summary(mut self, summarize: bool) -> Self {
        self.summarize = summarize;
        self
    }

    /// Whether the command's output may be block-buffered.
    pub fn buffered_output(&self) -> bool {
        self.command.buffered_output()
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    /// Tag fetch counters, when filtering on tags.
    pub fn tag_stats(&self) -> Option<&TagFetchStats> {
        self.tag_stage.as_ref().map(|stage| stage.fetcher.stats())
    }

    /// Lists objects and runs the command on every match, writing output to
    /// `out`. Returns summary statistics if enabled.
    pub async fn run(&self, out: &mut (dyn Write + Send)) -> S3FindResult<Option<FindStat>> {
        let objects = list_objects(&self.client, &self.listing);
        self.process(objects, out).await
    }

    /// Runs the pipeline over already-listed objects.
    ///
    /// Matching runs ahead of the command by up to one batch, so listing and
    /// tag fetching continue while a batch is being executed.
    pub async fn process<'a>(
        &'a self,
        objects: impl Stream<Item = S3FindResult<StreamObject>> + Send + 'a,
        out: &mut (dyn Write + Send),
    ) -> S3FindResult<Option<FindStat>> {
        let (sender, receiver) = mpsc::channel(BATCH_SIZE);
        // Ends early, without error, once the receiver is gone.
        let produce = self
            .matches(objects)
            .map(Ok)
            .forward(sender)
            .map(|_| Ok(()));
        let consume = self.consume(receiver, out);

        let ((), stats) = try_join(produce, consume).await?;
        Ok(stats)
    }

    /// Runs the command on matches in batches of up to [`BATCH_SIZE`],
    /// stopping at the first error.
    async fn consume(
        &self,
        mut matches: impl Stream<Item = S3FindResult<StreamObject>> + Unpin,
        out: &mut (dyn Write + Send),
    ) -> S3FindResult<Option<FindStat>> {
        let mut stats = self.summarize.then(FindStat::default);
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        while let Some(object) = matches.try_next().await? {
            batch.push(object);
            if batch.len() == BATCH_SIZE {
                self.execute(&batch, stats.as_mut(), out).await?;
                batch.clear();
            }
        }

        if !batch.is_empty() {
            self.execute(&batch, stats.as_mut(), out).await?;
        }

        Ok(stats)
    }

    /// Objects passing every filter, up to the limit.
    ///
    /// The stream is lazy: once the limit is reached nothing more is listed,
    /// and tags are only fetched for objects that passed the cheap filters.
    fn matches<'a>(
        &'a self,
        objects: impl Stream<Item = S3FindResult<StreamObject>> + Send + 'a,
    ) -> impl Stream<Item = S3FindResult<StreamObject>> + Send + 'a {
        let candidates = objects.try_filter(move |object| ready(self.filters.matches(object)));

        let matches = match &self.tag_stage {
            None => candidates.left_stream(),
            Some(stage) => stage
                .fetcher
                .attach_tags_to_stream(&self.client, candidates)
                .try_filter(move |object| ready(stage.filters.matches(object)))
                .right_stream(),
        };

        matches.take(self.limit.unwrap_or(usize::MAX))
    }

    async fn execute(
        &self,
        batch: &[StreamObject],
        stats: Option<&mut FindStat>,
        out: &mut (dyn Write + Send),
    ) -> S3FindResult<()> {
        let mut ctx = CommandContext {
            client: &self.client,
            path: &self.listing.path,
            out,
            concurrency: self.concurrency,
        };
        self.command
            .execute(&mut ctx, batch)
            .await
            .map_err(S3FindError::command_execution)?;
        ctx.out.flush().map_err(S3FindError::command_execution)?;

        if let Some(stats) = stats {
            for object in batch {
                stats.record(&object.object);
            }
        }
        Ok(())
    }
}

/// Notes about option combinations, for the CLI to print before running.
pub fn option_warnings(opts: &FindOpt) -> Vec<&'static str> {
    let mut warnings = Vec::new();

    if opts.all_versions && opts.maxdepth.is_some() {
        warnings.push("Warning: --maxdepth is ignored when --all-versions is used");
    }

    let filters_on_tags = !opts.tag.is_empty() || !opts.tag_exists.is_empty();
    if filters_on_tags && opts.limit.is_none() {
        warnings.push(
            "Note: Tag filtering requires an API call per object. \
            Consider using --limit to restrict the number of objects processed.",
        );
    }

    warnings
}

#[cfg(test)]
mod tests;
