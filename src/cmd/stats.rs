use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Result};
use console::style;
use serde::Serialize;
use structopt::StructOpt;
use thousands::Separable;

use super::util::{expand_dirs, DataExecutor, DataInstance};
use crate::tokens::{tokenize, PretrainedTokenizer};
use crate::util;

#[derive(Debug, StructOpt, Clone)]
pub(crate) struct Opt {
    /// Path to a gzip-compressed JSON lines file.
    #[structopt(parse(from_os_str))]
    path: Vec<PathBuf>,

    /// Limit the number of JSON lines per file to process.
    #[structopt(short = "l", long = "limit")]
    limit: Option<usize>,

    /// Limit the number of files to process.
    #[structopt(long = "file-limit")]
    file_limit: Option<usize>,

    /// Set the max number of threads/workers to use. Defaults to min(64, num CPU).
    #[structopt(short = "j", long = "workers")]
    workers: Option<usize>,

    /// A path to write the JSON output to.
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
    opt.path = expand_dirs(&opt.path)?;
    if opt.path.is_empty() {
        bail!("at least one path is required");
    }
    if let Some(file_limit) = opt.file_limit {
        opt.path.truncate(file_limit);
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

    let stats: Stats<Arc<AtomicUsize>> = Stats::default();

    let mut executor =
        DataExecutor::new(&opt.path, opt.workers, opt.limit, "Collecting", opt.quiet)?;
    executor.max_retries = 2;

    for path in &opt.path {
        let sync_stats_callback = {
            let stats = stats.clone();
            move |mut local_stats: LocalStats| -> Result<()> {
                // Update counts.
                stats
                    .total_tokens
                    .fetch_add(local_stats.total_tokens, Ordering::Relaxed);
                stats
                    .total_documents
                    .fetch_add(local_stats.total_documents, Ordering::Relaxed);
                stats
                    .document_max_tokens
                    .fetch_max(local_stats.document_max_tokens, Ordering::Relaxed);
                stats
                    .document_min_tokens
                    .fetch_min(local_stats.document_min_tokens, Ordering::Relaxed);

                // Prune max/min token document pointers.
                stats.prune_documents()?;

                // Sync max token document pointers.
                let current_max = stats.document_max_tokens.load(Ordering::Relaxed);
                let mut max_token_documents = stats
                    .max_token_documents
                    .lock()
                    .map_err(|_| anyhow!("Failed to acquire lock"))?;
                for doc_pointer in local_stats.max_token_documents.drain(0..) {
                    if doc_pointer.num_tokens >= current_max {
                        (*max_token_documents).push_back(doc_pointer);
                    }
                }

                // Sync min token document pointers.
                let current_min = stats.document_min_tokens.load(Ordering::Relaxed);
                let mut min_token_documents = stats
                    .min_token_documents
                    .lock()
                    .map_err(|_| anyhow!("Failed to acquire lock"))?;
                for doc_pointer in local_stats.min_token_documents.drain(0..) {
                    if doc_pointer.num_tokens <= current_min {
                        (*min_token_documents).push_back(doc_pointer);
                    }
                }

                Ok(())
            }
        };
        let local_stats_factory = {
            let stats = stats.clone();
            move || -> Result<LocalStats> {
                Ok(LocalStats {
                    document_max_tokens: stats.document_max_tokens.load(Ordering::Relaxed),
                    document_min_tokens: stats.document_min_tokens.load(Ordering::Relaxed),
                    ..Default::default()
                })
            }
        };
        let tokenizer = tokenizer.clone();
        executor.execute_with_callback(
            path,
            move |data: DataInstance,
                  path: &Path,
                  line_num: usize,
                  local_stats: &mut LocalStats|
                  -> Result<()> {
                local_stats.total_documents += 1;
                if let Some(text) = data.text {
                    let mut num_tokens = 0;

                    if let Some(ref tokenizer) = tokenizer {
                        let tokens = tokenizer.tokenize(&text)?;
                        num_tokens += tokens.len();
                    } else {
                        for _ in tokenize(&text) {
                            num_tokens += 1;
                        }
                    }

                    local_stats.total_tokens += num_tokens;
                    local_stats.document_max_tokens =
                        std::cmp::max(num_tokens, local_stats.document_max_tokens);
                    local_stats.document_min_tokens =
                        std::cmp::min(num_tokens, local_stats.document_min_tokens);
                    if num_tokens == local_stats.document_max_tokens {
                        local_stats.max_token_documents.push(DocumentPointer {
                            path: path.into(),
                            line: line_num,
                            num_tokens,
                        });
                    }
                    if num_tokens == local_stats.document_min_tokens {
                        local_stats.min_token_documents.push(DocumentPointer {
                            path: path.into(),
                            line: line_num,
                            num_tokens,
                        });
                    }
                }

                Ok(())
            },
            local_stats_factory,
            sync_stats_callback,
        )?;
    }

    executor.join()?;
    stats.total_bytes.store(
        executor.total_bytes.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    stats.prune_documents()?;

    let json_out = serde_json::to_string(&stats)?;

    if opt.json {
        println!("{json_out}");
    } else if !opt.quiet {
        for (name, value) in stats.get_display_values() {
            println!("{}: {}", style(name).cyan(), value);
        }

        // Show max token documents.
        println!("{}:", style("max token documents").cyan());
        let max_token_documents = stats
            .max_token_documents
            .lock()
            .map_err(|_| anyhow!("Failed to acquire lock"))?;
        for doc_pointer in (*max_token_documents).iter() {
            println!("  - {}: {:?}", style("path").cyan(), doc_pointer.path);
            println!("    {}: {}", style("line").cyan(), doc_pointer.line);
            println!("    {}: {}", style("tokens").cyan(), doc_pointer.num_tokens);
        }

        // Show min token documents.
        println!("{}:", style("min token documents").cyan());
        let min_token_documents = stats
            .min_token_documents
            .lock()
            .map_err(|_| anyhow!("Failed to acquire lock"))?;
        for doc_pointer in (*min_token_documents).iter() {
            println!("  - {}: {:?}", style("path").cyan(), doc_pointer.path);
            println!("    {}: {}", style("line").cyan(), doc_pointer.line);
            println!("    {}: {}", style("tokens").cyan(), doc_pointer.num_tokens);
        }
    }

    if let Some(ref mut file) = out_file {
        writeln!(file, "{json_out}")?;
    }

    if let Some(path) = out_path {
        log::info!("Output written to {:?}", path);
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct DocumentPointer {
    path: PathBuf,
    line: usize,
    num_tokens: usize,
}

#[derive(Debug, Clone)]
struct LocalStats {
    total_tokens: usize,
    total_documents: usize,
    document_max_tokens: usize,
    document_min_tokens: usize,
    max_token_documents: Vec<DocumentPointer>,
    min_token_documents: Vec<DocumentPointer>,
}

impl Default for LocalStats {
    fn default() -> Self {
        Self {
            total_tokens: 0,
            total_documents: 0,
            document_max_tokens: 0,
            document_min_tokens: usize::MAX,
            max_token_documents: Vec::new(),
            min_token_documents: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Stats<T: std::fmt::Debug> {
    total_tokens: T,
    total_documents: T,
    total_bytes: T,
    document_max_tokens: T,
    document_min_tokens: T,
    max_token_documents: Arc<Mutex<VecDeque<DocumentPointer>>>,
    min_token_documents: Arc<Mutex<VecDeque<DocumentPointer>>>,
}

impl<T: std::fmt::Debug> Stats<T> {
    fn get_display_values(&self) -> Vec<(String, String)> {
        vec![
            (
                "total tokens".to_string(),
                format!("{:?}", self.total_tokens).separate_with_commas(),
            ),
            (
                "total documents".to_string(),
                format!("{:?}", self.total_documents).separate_with_commas(),
            ),
            (
                "total bytes".to_string(),
                format!("{:?}", self.total_bytes).separate_with_commas(),
            ),
            (
                "max tokens per document".to_string(),
                format!("{:?}", self.document_max_tokens).separate_with_commas(),
            ),
            (
                "min tokens per document".to_string(),
                format!("{:?}", self.document_min_tokens).separate_with_commas(),
            ),
        ]
    }
}

impl Stats<Arc<AtomicUsize>> {
    fn prune_documents(&self) -> Result<()> {
        let current_max = self.document_max_tokens.load(Ordering::Relaxed);
        let mut max_token_documents = self
            .max_token_documents
            .lock()
            .map_err(|_| anyhow!("Failed to acquire lock"))?;
        while let Some(doc_pointer) = (*max_token_documents).front() {
            if doc_pointer.num_tokens < current_max {
                (*max_token_documents).pop_front();
            } else {
                break;
            }
        }

        let current_min = self.document_min_tokens.load(Ordering::Relaxed);
        let mut min_token_documents = self
            .min_token_documents
            .lock()
            .map_err(|_| anyhow!("Failed to acquire lock"))?;
        while let Some(doc_pointer) = (*min_token_documents).front() {
            if doc_pointer.num_tokens > current_min {
                (*min_token_documents).pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }
}

impl Default for Stats<Arc<AtomicUsize>> {
    fn default() -> Self {
        Self {
            total_tokens: Arc::new(AtomicUsize::new(0)),
            total_documents: Arc::new(AtomicUsize::new(0)),
            total_bytes: Arc::new(AtomicUsize::new(0)),
            document_max_tokens: Arc::new(AtomicUsize::new(0)),
            document_min_tokens: Arc::new(AtomicUsize::new(usize::MAX)),
            max_token_documents: Arc::new(Mutex::new(VecDeque::new())),
            min_token_documents: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
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
