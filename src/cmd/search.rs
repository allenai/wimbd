use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::RandomState;
use anyhow::{bail, Result};
use console::style;
use regex::Regex;
use serde_json::json;
use structopt::StructOpt;

use super::util::{expand_dirs, DataExecutor, DataInstance};
use crate::util;

#[derive(Debug, StructOpt, Clone)]
pub(crate) struct Opt {
    /// Path to a gzip-compressed JSON lines file.
    #[structopt(parse(from_os_str))]
    path: Vec<PathBuf>,

    /// Pattern to search for. See https://docs.rs/regex/latest/regex/#syntax for the supported syntax.
    #[structopt(short = "p", long = "pattern", number_of_values = 1)]
    pattern: Vec<String>,

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
}

pub(crate) fn main(mut opt: Opt) -> Result<()> {
    opt.path = expand_dirs(&opt.path)?;

    if opt.pattern.is_empty() {
        bail!("At least one -p/--pattern regex term is required");
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

    let mut counts: HashMap<String, Arc<AtomicUsize>, RandomState> =
        HashMap::with_capacity_and_hasher(opt.pattern.len(), RandomState::new());
    let mut patterns: HashMap<String, Regex, RandomState> =
        HashMap::with_capacity_and_hasher(opt.pattern.len(), RandomState::new());
    for pattern in &opt.pattern {
        counts.insert(pattern.to_string(), Arc::new(AtomicUsize::new(0)));
        patterns.insert(pattern.to_string(), Regex::new(pattern)?);
    }

    let (mut out_file, out_path) = match get_output_file(&opt)? {
        Some(out) => (Some(out.0), Some(out.1)),
        None => (None, None),
    };

    let executor = DataExecutor::new(&opt.path, opt.workers, opt.limit, "Searching", opt.quiet)?;

    for path in &opt.path {
        let counts = counts.clone();
        let patterns = patterns.clone();
        executor.execute(
            path,
            move |data: DataInstance, _: &Path, _: usize| -> Result<()> {
                if let Some(text) = data.text {
                    for (pattern, regex) in &patterns {
                        for _ in regex.find_iter(&text) {
                            counts.get(pattern).unwrap().fetch_add(1, Ordering::Relaxed);
                        }
                    }
                };
                Ok(())
            },
        )?;
    }

    executor.join()?;

    for (i, (pattern, count)) in counts.iter().enumerate() {
        let count = count.load(Ordering::Relaxed);

        let json_out = &json!({
            "pattern": pattern,
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
                style(pattern).cyan(),
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
