use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::time::Duration;

use ahash::RandomState;
use anyhow::{bail, Result};
use console::style;
use regex::Regex;
use serde::Serialize;
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

    /// Include the exact location of each match in the output.
    #[structopt(long = "--with-locations")]
    with_locations: bool,

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
    let mut match_locations: Option<
        HashMap<String, HashMap<PathBuf, Vec<MatchLocation>, RandomState>, RandomState>,
    > = None;
    let mut tx: Option<SyncSender<(String, PathBuf, Vec<MatchLocation>)>> = None;
    let mut rx: Option<Receiver<(String, PathBuf, Vec<MatchLocation>)>> = None;
    if opt.with_locations {
        let (tx_, rx_) = sync_channel::<(String, PathBuf, Vec<MatchLocation>)>(512_000);
        tx = Some(tx_);
        rx = Some(rx_);
        match_locations = Some(HashMap::with_capacity_and_hasher(
            opt.pattern.len(),
            RandomState::new(),
        ));
    }

    for pattern in &opt.pattern {
        counts.insert(pattern.to_string(), Arc::new(AtomicUsize::new(0)));
        patterns.insert(pattern.to_string(), Regex::new(pattern)?);
        if let Some(ref mut locations) = match_locations {
            locations.insert(
                pattern.to_string(),
                HashMap::with_hasher(RandomState::new()),
            );
        }
    }

    let (mut out_file, out_path) = match get_output_file(&opt)? {
        Some(out) => (Some(out.0), Some(out.1)),
        None => (None, None),
    };

    let executor = DataExecutor::new(&opt.path, opt.workers, opt.limit, "Searching", opt.quiet)?;

    for path in &opt.path {
        let counts = counts.clone();
        let patterns = patterns.clone();

        let sync_match_locations = {
            let tx = tx.clone();
            let path = path.clone();
            move |local_match_locations: Option<
                HashMap<String, Vec<MatchLocation>, RandomState>,
            >|
                  -> Result<()> {
                if let Some(mut local_match_locations) = local_match_locations {
                    let tx = tx.as_ref().unwrap();
                    for (pattern, matches) in local_match_locations.drain() {
                        tx.send((pattern, path.clone(), matches))?;
                    }
                }
                Ok(())
            }
        };

        let local_match_locations_factory = {
            let opt = opt.clone();
            move || -> Result<Option<HashMap<String, Vec<MatchLocation>, RandomState>>> {
                let mut local_match_locations: Option<
                    HashMap<String, Vec<MatchLocation>, RandomState>,
                > = None;
                if opt.with_locations {
                    local_match_locations = Some(HashMap::with_capacity_and_hasher(
                        opt.pattern.len(),
                        RandomState::new(),
                    ));
                    for pattern in &opt.pattern {
                        local_match_locations
                            .as_mut()
                            .unwrap()
                            .insert(pattern.into(), Vec::new());
                    }
                }
                Ok(local_match_locations)
            }
        };

        executor.execute_with_callback(
            path,
            move |data: DataInstance,
                  _: &Path,
                  line_num: usize,
                  local_match_locations: &mut Option<
                HashMap<String, Vec<MatchLocation>, RandomState>,
            >|
                  -> Result<()> {
                if let Some(text) = data.text {
                    for (pattern, regex) in &patterns {
                        for m in regex.find_iter(&text) {
                            counts.get(pattern).unwrap().fetch_add(1, Ordering::Relaxed);
                            if let Some(ref mut locations) = local_match_locations {
                                let match_location = MatchLocation {
                                    line: line_num,
                                    start_col: m.start(),
                                    end_col: m.end(),
                                };
                                locations.get_mut(pattern).unwrap().push(match_location);
                            }
                        }
                    }
                };
                Ok(())
            },
            local_match_locations_factory,
            sync_match_locations,
        )?;
    }

    drop(tx);

    while !executor.done() {
        if let Some(ref rx) = rx {
            while let Ok((pattern, path, matches)) = rx.recv_timeout(Duration::from_secs(1)) {
                let matches_for_pattern: &mut HashMap<
                    PathBuf,
                    Vec<MatchLocation>,
                    ahash::RandomState,
                > = match_locations
                    .as_mut()
                    .map(|m| m.get_mut(&pattern).unwrap())
                    .unwrap();
                if !matches.is_empty() {
                    matches_for_pattern.insert(path.clone(), matches);
                }
            }
        }
    }

    executor.join()?;

    for (i, (pattern, count)) in counts.iter().enumerate() {
        let count = count.load(Ordering::Relaxed);
        let matches_for_pattern = match_locations.as_ref().map(|m| m.get(pattern).unwrap());

        let json_out = &json!({
            "pattern": pattern,
            "count": count,
            "matches": matches_for_pattern,
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
            if let Some(locations) = matches_for_pattern {
                for (path, locs) in locations.iter() {
                    if locs.is_empty() {
                        continue;
                    }
                    println!("  {}", style(path.to_string_lossy()).blue());
                    for loc in locs.iter() {
                        println!(
                            "    → line={}, start_col={}, end_col={}",
                            loc.line, loc.start_col, loc.end_col
                        );
                    }
                }
            }
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

#[derive(Debug, Clone, Serialize)]
struct MatchLocation {
    line: usize,
    start_col: usize,
    end_col: usize,
}
