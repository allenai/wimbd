use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::RandomState;
use anyhow::{bail, Result};
use console::style;
use serde_json::json;
use structopt::StructOpt;

use super::util::{DataExecutor, DataInstance};
use crate::tokens::{tokenize, PretrainedTokenizer};
use crate::util;

#[derive(Debug, StructOpt, Clone)]
pub(crate) struct Opt {
    /// Path to a gzip-compressed JSON lines file.
    #[structopt(parse(from_os_str))]
    path: Vec<PathBuf>,

    /// String to search for.
    #[structopt(short = "s", long = "search", number_of_values = 1)]
    search: Vec<String>,

    /// Limit the number of JSON lines per file to process.
    #[structopt(short = "l", long = "limit")]
    limit: Option<usize>,

    /// Limit the number of files to process.
    #[structopt(long = "file-limit")]
    file_limit: Option<usize>,

    /// Set the max number of threads/workers to use. Defaults to min(64, num CPU).
    #[structopt(short = "j", long = "workers")]
    workers: Option<usize>,

    /// A path to write the output to. Output will be written as JSON lines, i.e.
    /// each line will be a JSON object with the keys "search" and "count".
    ///
    /// If the file already exists and you want to overwrite it, use the '-f/--force' option.
    #[structopt(short = "o", long = "out")]
    out: Option<PathBuf>,

    /// Don't show progress bars. Additionally, if an output file is specified nothing will be written to stdout.
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
}

pub(crate) fn main(mut opt: Opt) -> Result<()> {
    if opt.search.is_empty() {
        bail!("At least one -s/--search term is required");
    }
    if let Some(file_limit) = opt.file_limit {
        if file_limit == 0 {
            bail!("File limit cannot be 0");
        }
        opt.path.truncate(file_limit);
    }
    if opt.path.is_empty() {
        bail!("at least one path is required");
    }

    let tokenizer: Option<PretrainedTokenizer> = if &opt.tokenizer == "unicode" {
        None
    } else {
        Some(PretrainedTokenizer::new(&opt.tokenizer)?)
    };

    let mut counts: HashMap<Vec<String>, Arc<AtomicUsize>, RandomState> =
        HashMap::with_capacity_and_hasher(opt.search.len(), RandomState::new());
    let mut min_search_length = usize::MAX;
    for search in &opt.search {
        let search_tokens: Vec<String> = if let Some(ref tokenizer) = tokenizer {
            tokenizer.tokenize(search)?
        } else {
            tokenize(search).map(|t| t.into()).collect()
        };
        min_search_length = std::cmp::min(min_search_length, search_tokens.len());
        counts.insert(search_tokens, Arc::new(AtomicUsize::new(0)));
    }

    let (mut out_file, out_path) = match get_output_file(&opt)? {
        Some(out) => (Some(out.0), Some(out.1)),
        None => (None, None),
    };

    let executor = DataExecutor::new(&opt.path, opt.workers, opt.limit, "Searching", opt.quiet)?;

    for path in &opt.path {
        let counts = counts.clone();

        if let Some(ref tokenizer) = tokenizer {
            let tokenizer = (*tokenizer).clone();

            executor.execute(
                path,
                move |data: DataInstance, _: &Path, _: usize| -> Result<()> {
                    if let Some(text) = data.text {
                        let tokens = tokenizer.tokenize(&text)?;
                        count_occurences(min_search_length, tokens, &counts);
                    };
                    Ok(())
                },
            )?;
        } else {
            executor.execute(
                path,
                move |data: DataInstance, _: &Path, _: usize| -> Result<()> {
                    if let Some(text) = data.text {
                        let tokens: Vec<&str> = tokenize(&text).collect();
                        count_occurences(min_search_length, tokens, &counts);
                    };
                    Ok(())
                },
            )?;
        }
    }

    executor.join()?;

    for (i, (search, count)) in counts.iter().enumerate() {
        let count = count.load(Ordering::Relaxed);

        let search_str = if let Some(ref tokenizer) = tokenizer {
            tokenizer.decode(search)?
        } else {
            search.join(" ")
        };
        let json_out = &json!({
            "tokens": search,
            "string": search_str,
            "count": count,
        })
        .to_string();

        if opt.json {
            println!("{json_out}");
        } else if !opt.quiet {
            println!(
                "[{}/{}] {:?} (count = {})",
                i + 1,
                counts.len(),
                style(search_str).cyan(),
                count
            );
        }

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
        if path.is_dir() {
            bail!("-o/--out must be a valid file name, not a directory");
        } else {
            Ok(Some(util::get_output_file(path, opt.force)?))
        }
    } else {
        Ok(None)
    }
}

fn count_occurences<T>(
    min_search_length: usize,
    tokens: Vec<T>,
    counts: &HashMap<Vec<String>, Arc<AtomicUsize>, RandomState>,
) where
    T: std::cmp::PartialEq<String>,
{
    for index in min_search_length..(tokens.len() + 1) {
        for (search, count) in counts.iter() {
            if search.len() <= index {
                let slice = &tokens[(index - search.len())..index];
                if slice == &search[..] {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
