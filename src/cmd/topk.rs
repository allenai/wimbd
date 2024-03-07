use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use atomic_traits::{Atomic, NumOps};
use console::style;
use num_traits::{Bounded, NumCast, One, SaturatingSub, Zero};
use serde_json::json;
use structopt::StructOpt;

use super::util::{expand_dirs, parse_size_default_to_gb, DataExecutor, DataInstance};
use crate::ngrams::{NgramCounter, TopKNgrams};
use crate::tokens::{tokenize, PretrainedTokenizer};
use crate::util;

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

    /// The number of top ngrams to return.
    #[structopt(short = "k", long = "topk", default_value = "20")]
    topk: usize,

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

    /// A path to write the output to. Output will be written as JSON lines, i.e.
    /// each line will be a JSON object with the keys "ngram" and "count".
    ///
    /// If given a valid file name, the output will be written to that file. If the file
    /// already exists and you want to overwrite it, use the '-f/--force' option.
    ///
    /// You can also give a directory name, in which case a descriptive file name will be generated.
    #[structopt(short = "o", long = "out")]
    out: Option<PathBuf>,

    /// Don't show progress bars and minimize other output.
    /// This doesn't affect logging.
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Format output as JSON.
    #[structopt(long = "json")]
    json: bool,

    /// Force overwriting output file if it already exists.
    #[structopt(short = "f", long = "force")]
    force: bool,

    /// Set the tokenizer to use. This can be the name of a pretrained tokenizer
    /// from HuggingFace.
    #[structopt(short = "t", long = "tokenizer", default_value = "unicode")]
    tokenizer: String,

    /// Set a minimum count threshold for ngrams to be considered for the top-k.
    /// Setting a high threshold can improve speed, but be careful not to set a threshold
    /// higher than what you expect the minimum count in the top-k to be.
    #[structopt(long = "threshold", default_value = "1")]
    threshold: u32,

    /// Use u64 integers instead of u32 integers in the hash table.
    /// The doubles the memory requirements for a given hash table size and therefore increases the
    /// probability of hash collisions for a given memory budget, but may be useful when the topk
    /// ngram counts exceed the maximum value representable by u32 integers.
    /// Note that overflows are always guarded against by capping the counts to the data type max.
    #[structopt(long = "u64")]
    use_u64: bool,
}

pub(crate) fn main(mut opt: Opt) -> Result<()> {
    opt.path = expand_dirs(&opt.path)?;

    // Validate arguments.
    if opt.topk == 0 {
        bail!("-k/--topk must be greater than 0");
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

    if opt.use_u64 {
        topk::<AtomicU64>(opt)
    } else {
        topk::<AtomicU32>(opt)
    }
}

fn topk<A>(opt: Opt) -> Result<()>
where
    A: Atomic + NumOps + Send + Sync + 'static,
    <A as Atomic>::Type: Zero
        + One
        + Bounded
        + NumCast
        + Ord
        + SaturatingSub
        + Copy
        + Clone
        + AddAssign<<A as Atomic>::Type>
        + Sync
        + Send
        + std::fmt::Display
        + serde::Serialize,
{
    let mut topk: TopKNgrams<String, A> = TopKNgrams::new(opt.topk);
    let (tx, rx) = sync_channel::<(Vec<String>, <A as Atomic>::Type)>(512_000);

    let tokenizer: Option<PretrainedTokenizer> = if &opt.tokenizer == "unicode" {
        None
    } else {
        Some(PretrainedTokenizer::new(&opt.tokenizer)?)
    };

    let (mut out_file, out_path) = match get_output_file(&opt)? {
        Some(out) => (Some(out.0), Some(out.1)),
        None => (None, None),
    };

    log::info!("Initializing ngram counter...");
    // We're storing an array of u32 or u64s.
    // Each u32 is 32 bits of memory, or 4 bytes.
    // Each u64 is 64 bits of memory, or 8 bytes.
    // So we divide the size by 4 or 8 to get the length of the array.
    let counter_size = if opt.use_u64 {
        opt.size / 8
    } else {
        opt.size / 4
    };
    let ngram_counts: Arc<NgramCounter<A>> = Arc::new(NgramCounter::new(
        counter_size as usize,
        opt.hashes as usize,
        opt.seed,
        <A as Atomic>::Type::zero(),
    )?);

    log::info!("Counting ngrams...");

    let executor = DataExecutor::new(
        &opt.path,
        opt.workers,
        opt.limit,
        "Counting ngrams",
        opt.quiet,
    )?;

    // Send work to threads. Each job reads a file, collects ngrams, increments each ngram's global count,
    // and then collects it's own local top-k which it will merge with the global top-k after
    // processing the file.
    // The fact that each worker uses the current global counts for each ngram to fill its local
    // top-k ensures that the final top-k will be correct (ignoring hash collisions in Bloom
    // counter).
    for path in &opt.path {
        // This is our function that collects/counts ngrams from a data line.
        let collect_ngrams = {
            let tokenizer = tokenizer.clone();
            let ngram_counts = ngram_counts.clone();
            let min_count = topk.min_count();
            let threshold = <<A as Atomic>::Type as NumCast>::from(opt.threshold).unwrap();

            move |data: DataInstance,
                  _: &Path,
                  _: usize,
                  local_topk: &mut TopKNgrams<String, A>|
                  -> Result<()> {
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
                            let count: <A as Atomic>::Type =
                                ngram_counts.increment(&ngram_deque, <A as Atomic>::Type::one());
                            if count > threshold
                                && count >= local_topk.min_count
                                && count >= min_count.load(Ordering::Relaxed)
                            {
                                let ngram: Vec<String> = ngram_deque.iter().cloned().collect();
                                local_topk.insert(ngram, count);
                            }
                        }
                    }
                }

                Ok(())
            }
        };

        // This callback will be invoked at the end of a file to merge the local top-k with the
        // global top-k.
        let sync_local_topk_callback = {
            let min_count = topk.min_count();
            let threshold = <<A as Atomic>::Type as NumCast>::from(opt.threshold).unwrap();
            let tx = tx.clone();

            move |mut local_topk: TopKNgrams<String, A>| -> Result<()> {
                for (ngram, count) in local_topk.drain() {
                    if count > threshold && count >= min_count.load(Ordering::Relaxed) {
                        tx.send((ngram.to_vec(), count))?;
                    }
                }
                Ok(())
            }
        };

        // This is just for initializing the local top-k.
        let local_topk_factory = move || -> Result<TopKNgrams<String, A>> {
            let topk: TopKNgrams<String, A> = TopKNgrams::new(opt.topk);
            Ok(topk)
        };

        executor.execute_with_callback(
            path,
            collect_ngrams,
            local_topk_factory,
            sync_local_topk_callback,
        )?;
    }

    drop(tx);

    // Collect ngrams and counts from channel until all jobs are done.
    while !executor.done() {
        while let Ok((ngram, count)) = rx.recv_timeout(Duration::from_secs(1)) {
            topk.insert(ngram, count);
            if executor.has_errors() {
                break;
            }
        }
    }

    executor.join()?;

    let mut warn_about_overflows = false;

    let topk_final = topk.drain();
    for (i, (ngram, count)) in topk_final.iter().enumerate() {
        // Check for overflow.
        if *count == <A as Atomic>::Type::max_value() {
            warn_about_overflows = true;
        }

        let ngram_str = if let Some(ref tokenizer) = tokenizer {
            tokenizer.decode(ngram)?
        } else {
            ngram.join(" ")
        };
        let json_out = &json!({
            "tokens": **ngram,
            "string": ngram_str,
            "count": count,
            "rank": i + 1,
        })
        .to_string();

        // Display output.
        if opt.json {
            println!("{json_out}");
        } else if opt.out.is_none() {
            println!(
                "[{}/{}] {:?} (count ≤ {})",
                i + 1,
                topk_final.len(),
                style(ngram_str).cyan(),
                count,
            );
        }

        // Write ngram and count to file.
        if let Some(ref mut file) = out_file {
            writeln!(file, "{json_out}")?;
        }
    }

    if topk_final.is_empty() {
        log::warn!("No ngrams occurred more than once, topk is empty");
    }

    if warn_about_overflows {
        log::warn!("u32 overflow in ngram counts");
    }

    if let Some(path) = out_path {
        log::info!("Output written to {:?}", path);
    }

    Ok(())
}

fn get_output_file(opt: &Opt) -> Result<Option<(File, PathBuf)>> {
    if let Some(path) = &opt.out {
        if path.is_dir() || path.extension().is_none() {
            let mut parts = vec![format!("n{}-k{}-h{}", opt.ngram, opt.topk, opt.hashes)];
            if let Some(limit) = opt.limit {
                parts.push(format!("-limit{limit}"));
            }
            if let Some(seed) = opt.seed {
                parts.push(format!("-seed{seed}"));
            }
            Ok(Some(util::get_output_file(
                path.join(format!("{}.jsonl", parts.join("-"))),
                opt.force,
            )?))
        } else {
            Ok(Some(util::get_output_file(path, opt.force)?))
        }
    } else {
        Ok(None)
    }
}
