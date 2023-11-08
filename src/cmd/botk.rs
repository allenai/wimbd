use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use atomic_traits::Atomic;
use console::style;
use num_traits::{NumCast, One};
use rand::{random, rngs::StdRng, seq::SliceRandom, SeedableRng};
use serde_json::json;
use structopt::StructOpt;

use super::util::{parse_size_default_to_gb, DataExecutor, DataInstance};
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

    /// The number of least common ngrams to return.
    #[structopt(short = "k", default_value = "20")]
    k: usize,

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

    /// Set a maximum count threshold for ngrams to be considered for the bottom-k.
    /// Setting a lower threshold can improve speed, but be careful not to set a threshold
    /// lower than what you expect the maximum count in the bottom-k to be.
    #[structopt(long = "threshold", default_value = "4294967295")]
    threshold: u32,

    /// Add more randomness to the results by specifying a probably of keeping each rare ngram
    /// encountered.
    #[structopt(long = "--p-keep")]
    p_keep: Option<f32>,
}

pub(crate) fn main(mut opt: Opt) -> Result<()> {
    // Validate arguments.
    if opt.path.is_empty() {
        bail!("at least one path is required");
    }
    if opt.k == 0 {
        bail!("-k must be greater than 0");
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
    if let Some(p_keep) = opt.p_keep {
        if p_keep <= 0.0 || p_keep > 1.0 {
            bail!("--p-keep must be between in the interval (0, 1]");
        }
    }

    let tokenizer: Option<PretrainedTokenizer> = if &opt.tokenizer == "unicode" {
        None
    } else {
        Some(PretrainedTokenizer::new(&opt.tokenizer)?)
    };

    let (mut out_file, out_path) = match get_output_file(&opt)? {
        Some(out) => (Some(out.0), Some(out.1)),
        None => (None, None),
    };

    // Shuffle paths.
    let mut rng = if let Some(seed) = opt.seed {
        StdRng::seed_from_u64(seed)
    } else {
        StdRng::from_entropy()
    };
    opt.path.shuffle(&mut rng);

    log::info!("Initializing ngram counter...");
    // We're storing an array of u32s, so each u32 is 32 bits of memory, or 4 bytes.
    // So we divide the size by 4 to get the length of the array.
    let counter_size = opt.size / 4;
    let ngram_counts = Arc::new(NgramCounter::<AtomicU32>::new(
        counter_size as usize,
        opt.hashes as usize,
        opt.seed,
        u32::MAX,
    )?);

    let executor = DataExecutor::new(
        &opt.path,
        opt.workers,
        opt.limit,
        "Counting ngrams",
        opt.quiet,
    )?;

    // First pass through the data: each job reads a file, collects ngrams and decrements their count
    // from u32::MAX.
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
                            ngram_counts
                                .decrement(&ngram_deque, <AtomicU32 as Atomic>::Type::one());
                        }
                    }
                }

                Ok(())
            }
        };

        executor.execute(path, collect_ngrams)?;
    }

    executor.join()?;

    let executor = DataExecutor::new(
        &opt.path,
        opt.workers,
        opt.limit,
        "Collecting ngrams",
        opt.quiet,
    )?;
    let mut topk: TopKNgrams<String, AtomicU32> = TopKNgrams::new(opt.k);
    let (tx, rx) = sync_channel(512_000);

    // Second pass through the data: collect ngrams and add to the top-k (bottom-k)
    // if their "inverse count" is high enough.
    for path in &opt.path {
        let collect_ngrams = {
            let tokenizer = tokenizer.clone();
            let ngram_counts = ngram_counts.clone();
            let min_count = topk.min_count();
            let threshold = u32::MAX - opt.threshold;
            move |data: DataInstance,
                  _: &Path,
                  _: usize,
                  local_topk: &mut TopKNgrams<String, AtomicU32>|
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
                            let inverse_count = ngram_counts.max_count(&ngram_deque);
                            if inverse_count > threshold
                                && inverse_count >= local_topk.min_count
                                && inverse_count >= min_count.load(Ordering::Relaxed)
                            {
                                if let Some(p_keep) = opt.p_keep {
                                    if random::<f32>() > p_keep {
                                        continue;
                                    }
                                }
                                let ngram: Vec<String> = ngram_deque.iter().cloned().collect();
                                local_topk.insert(ngram, inverse_count);
                            }
                        }
                    }
                }

                Ok(())
            }
        };

        // This callback will be invoked at the end of a file to merge the local top-k (bottom-k) with the
        // global top-k.
        let sync_local_topk = {
            let min_count = topk.min_count();
            let threshold = <<AtomicU32 as Atomic>::Type as NumCast>::from(opt.threshold).unwrap();
            let tx = tx.clone();

            move |mut local_topk: TopKNgrams<String, AtomicU32>| -> Result<()> {
                for (ngram, inverse_count) in local_topk.drain() {
                    if inverse_count > threshold
                        && inverse_count >= min_count.load(Ordering::Relaxed)
                    {
                        tx.send((ngram.to_vec(), inverse_count))?;
                    }
                }
                Ok(())
            }
        };

        // This is just for initializing the local top-k.
        let local_topk_factory = move || -> Result<TopKNgrams<String, AtomicU32>> {
            let topk: TopKNgrams<String, AtomicU32> = TopKNgrams::new(opt.k);
            Ok(topk)
        };

        executor.execute_with_callback(
            path,
            collect_ngrams,
            local_topk_factory,
            sync_local_topk,
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

    let bottom_k_final = topk.drain();
    for (i, (ngram, inverse_count)) in bottom_k_final.iter().enumerate() {
        let count = u32::MAX - inverse_count;
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
                "[{}/{}] {:?} (count {} {})",
                i + 1,
                bottom_k_final.len(),
                style(ngram_str).cyan(),
                if count > 1 { "≤" } else { "=" },
                count,
            );
        }

        // Write ngram and count to file.
        if let Some(ref mut file) = out_file {
            writeln!(file, "{json_out}")?;
        }
    }

    if let Some(path) = out_path {
        log::info!("Output written to {:?}", path);
    }

    Ok(())
}

fn get_output_file(opt: &Opt) -> Result<Option<(File, PathBuf)>> {
    if let Some(path) = &opt.out {
        if path.is_dir() || path.extension().is_none() {
            let mut parts = vec![format!("n{}-k{}-h{}", opt.ngram, opt.k, opt.hashes)];
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
