use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU8;
use std::sync::Arc;

use anyhow::{bail, Result};
use serde_json::json;
use structopt::StructOpt;

use super::util::{expand_dirs, parse_size_default_to_gb, DataExecutor, DataInstance};
use crate::ngrams::NgramCounter;
use crate::tokens::{tokenize, PretrainedTokenizer};

#[derive(Debug, StructOpt, Clone)]
pub(crate) struct Opt {
    /// Path to a gzip-compressed JSON lines file.
    #[structopt(parse(from_os_str))]
    path: Vec<PathBuf>,

    /// Ngram size.
    #[structopt(short = "n", long = "ngram", default_value = "3")]
    ngram: usize,

    /// Limit the number of JSON lines per file to process.
    #[structopt(short = "l", long = "limit")]
    limit: Option<usize>,

    /// Limit the number of files to process.
    #[structopt(long = "file-limit")]
    file_limit: Option<usize>,

    /// Set the max number of threads/workers to use. Defaults to min(64, num CPU).
    #[structopt(short = "j", long = "workers")]
    workers: Option<usize>,

    /// Specify the size budget for the internal ngram counter hash table, e.g. "8GiB".
    /// In general it's best to choose the largest size that will fit in memory
    /// on your machine.
    #[structopt(long = "size", default_value = "4GiB", parse(try_from_str = parse_size_default_to_gb))]
    size: u64,

    /// Specify the number of hash functions to use.
    #[structopt(short = "h", long = "hashes", default_value = "5")]
    hashes: u8,

    /// Set the seed for the hashing functions. By default the seed is chosen at random.
    #[structopt(long = "seed")]
    seed: Option<u64>,

    /// Don't show progress bars and minimize other output.
    /// This doesn't affect logging.
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Format output as JSON.
    #[structopt(long = "json")]
    json: bool,

    /// Set the tokenizer to use. This can be the name of a pretrained tokenizer
    /// from HuggingFace.
    #[structopt(short = "t", long = "tokenizer", default_value = "unicode")]
    tokenizer: String,
}

pub(crate) fn main(mut opt: Opt) -> Result<()> {
    opt.path = expand_dirs(&opt.path)?;

    // Validate arguments.
    if opt.path.is_empty() {
        bail!("at least one path is required");
    }
    if opt.size == 0 {
        bail!("--size must be greater than 0");
    }
    if opt.hashes == 0 {
        bail!("-h/--hashes must be greater than 0");
    }
    if opt.ngram == 0 {
        bail!("-n/--ngram must be greater than 0");
    }
    if let Some(file_limit) = opt.file_limit {
        opt.path.truncate(file_limit);
    }

    let tokenizer: Option<PretrainedTokenizer> = if &opt.tokenizer == "unicode" {
        None
    } else {
        Some(PretrainedTokenizer::new(&opt.tokenizer)?)
    };

    log::info!("Initializing ngram counter...");
    // We're storing an array of u8s, so the size (in bytes) is also the length.
    let counter_size = opt.size;
    let ngram_counts = Arc::new(NgramCounter::<AtomicU8>::new(
        counter_size as usize,
        opt.hashes as usize,
        opt.seed,
        0,
    )?);

    let executor = DataExecutor::new(
        &opt.path,
        opt.workers,
        opt.limit,
        "Collecting ngrams",
        opt.quiet,
    )?;

    for path in &opt.path {
        // This is our function that collects ngrams from a data line.
        let collect_ngrams = {
            let tokenizer = tokenizer.clone();
            let ngram_counts = ngram_counts.clone();

            move |data: DataInstance, _: &Path, _: usize| -> Result<()> {
                if let Some(text) = data.text {
                    let tokens: Box<dyn Iterator<Item = String>> =
                        if let Some(tokenizer) = &tokenizer {
                            Box::new(tokenizer.tokenize(&text)?.into_iter())
                        } else {
                            Box::new(tokenize(&text).map(|s| s.to_string()))
                        };

                    let mut ngram_deque: VecDeque<String> = VecDeque::with_capacity(opt.ngram);
                    for token in tokens {
                        if ngram_deque.len() == opt.ngram {
                            ngram_deque.pop_front();
                        }

                        ngram_deque.push_back(token);

                        if ngram_deque.len() == opt.ngram {
                            ngram_counts.increment(&ngram_deque, 1);
                        }
                    }
                }

                Ok(())
            }
        };

        executor.execute(path, collect_ngrams)?;
    }

    executor.join()?;

    log::info!("Counting unique ngrams...");
    let unique_count = ngram_counts.nonzero();

    if opt.json {
        let json_out = &json!({
            "unique_count": unique_count,
        })
        .to_string();
        println!("{json_out}");
    } else {
        println!("Estimated number of unique ngrams: {}", unique_count);
    }

    Ok(())
}
