use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use glob::glob;
use humantime::format_duration;
use parse_size::parse_size;
use serde::Deserialize;
use thousands::Separable;
use threadpool::ThreadPool;

use crate::io::GzBufReader;
use crate::progress::{
    get_file_progress_bar, get_multi_progress_bar, get_progress_bar, MultiProgress, ProgressBar,
    ProgressIterator,
};

#[derive(Debug, Deserialize)]
pub(crate) struct DataInstance {
    // Unfortunately we can't just use a borrowed string here.
    // See https://github.com/serde-rs/serde/issues/1413#issuecomment-494892266
    pub(crate) text: Option<String>,
}

pub(crate) fn process_file<F, C, U, G>(
    mut data_func: F,
    context: C,
    mut callback: G,
    progress: Option<ProgressBar>,
    path: impl AsRef<Path>,
    limit: Option<usize>,
    early_exit: Arc<AtomicBool>,
) -> Result<(usize, usize)>
where
    F: FnMut(DataInstance, &Path, usize, &mut U) -> Result<()>,
    C: Fn() -> Result<U> + Send + 'static,
    G: FnMut(U) -> Result<()>,
{
    let mut total_lines: usize = 0;
    let mut total_bytes: usize = 0;
    let reader = GzBufReader::open(&path)?;
    let mut context = context()?;

    let mut process_line = |line: &str| -> Result<()> {
        if early_exit.load(Ordering::Relaxed) {
            return Ok(());
        }
        total_lines += 1;
        total_bytes += line.len();
        match serde_json::from_str(line) {
            Ok(data) => data_func(data, path.as_ref(), total_lines, &mut context),
            Err(e) => {
                if let Some(io_err) = e.io_error_kind() {
                    Err(io::Error::new(io_err, e).into())
                } else {
                    Err(e).with_context(|| {
                        format!(
                            "failed to deserialize line {} in {:?}:\n{}",
                            total_lines,
                            path.as_ref(),
                            line
                        )
                    })
                }
            }
        }
    };

    if let Some(limit) = limit {
        if let Some(progress) = progress {
            for line in reader.take(limit).progress_with(progress) {
                process_line(&line?)?;
            }
        } else {
            for line in reader.take(limit) {
                process_line(&line?)?;
            }
        }
    } else if let Some(progress) = progress {
        for line in reader.progress_with(progress) {
            process_line(&line?)?;
        }
    } else {
        for line in reader {
            process_line(&line?)?;
        }
    }

    callback(context)?;

    Ok((total_lines, total_bytes))
}

pub(crate) struct DataExecutor {
    all_progress: MultiProgress,
    file_progress: ProgressBar,
    pub(crate) total_lines: Arc<AtomicUsize>,
    pub(crate) total_bytes: Arc<AtomicUsize>,
    limit: Option<usize>,
    pool: ThreadPool,
    early_exit: Arc<AtomicBool>,
    start: Instant,
    error: Arc<Mutex<Option<String>>>,
    pub(crate) max_retries: usize,
    error_count: Arc<AtomicUsize>,
    max_workers: usize,
    quiet: bool,
}

impl DataExecutor {
    pub(crate) fn new(
        paths: &[PathBuf],
        max_workers: Option<usize>,
        limit: Option<usize>,
        description: &'static str,
        quiet: bool,
    ) -> Result<Self> {
        let all_progress = get_multi_progress_bar(quiet);
        let file_progress =
            all_progress.add(get_file_progress_bar(description, paths.len(), quiet)?);
        file_progress.set_position(0);
        let total_lines = Arc::new(AtomicUsize::new(0));
        let total_bytes = Arc::new(AtomicUsize::new(0));
        let workers = std::cmp::max(
            1,
            std::cmp::min(
                max_workers.unwrap_or_else(|| std::cmp::min(64, num_cpus::get())),
                paths.len(),
            ),
        );
        let pool = ThreadPool::with_name("wimbd-worker".to_string(), workers);
        let early_exit = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        let error = Arc::new(Mutex::new(None));
        Ok(Self {
            all_progress,
            file_progress,
            total_lines,
            total_bytes,
            limit,
            pool,
            early_exit,
            start,
            error,
            max_retries: 0,
            error_count: Arc::new(AtomicUsize::new(0)),
            max_workers: workers,
            quiet,
        })
    }

    pub(crate) fn execute<F>(&self, path: &PathBuf, mut data_func: F) -> Result<()>
    where
        F: FnMut(DataInstance, &Path, usize) -> Result<()> + Send + 'static + Clone,
    {
        self.execute_with_callback(
            path,
            move |data: DataInstance,
                  path: &Path,
                  line_num: usize,
                  _: &mut Option<bool>|
                  -> Result<()> { data_func(data, path, line_num) },
            || -> Result<Option<bool>> { Ok(None) },
            |_: Option<bool>| -> Result<()> { Ok(()) },
        )
    }

    pub(crate) fn execute_with_callback<F, C, U, G>(
        &self,
        path: &PathBuf,
        data_func: F,
        context: C,
        callback: G,
    ) -> Result<()>
    where
        F: FnMut(DataInstance, &Path, usize, &mut U) -> Result<()> + Send + 'static + Clone,
        C: Fn() -> Result<U> + Send + 'static + Clone,
        G: FnMut(U) -> Result<()> + Send + 'static + Clone,
    {
        if !path.is_file() {
            self.early_exit.store(true, Ordering::Relaxed);
            bail!("File {:?} does not exist", path);
        }

        let hide_file_progress = self.quiet || self.max_workers > 32;
        let path = path.clone();
        let total_lines = self.total_lines.clone();
        let total_bytes = self.total_bytes.clone();
        let progress = if hide_file_progress {
            None
        } else {
            Some(
                self.all_progress
                    .add(get_progress_bar(&path, self.limit, false)?),
            )
        };
        let limit = self.limit;
        let early_exit = self.early_exit.clone();
        let file_progress = self.file_progress.clone();
        let error = self.error.clone();
        let max_retries = self.max_retries;
        let error_count = self.error_count.clone();

        self.pool.execute(move || {
            let mut retries = 0;
            loop {
                match process_file(
                    data_func.clone(),
                    context.clone(),
                    callback.clone(),
                    progress.clone(),
                    &path,
                    limit,
                    early_exit.clone(),
                ) {
                    Ok((n_lines, n_bytes)) => {
                        total_lines.fetch_add(n_lines, Ordering::Relaxed);
                        total_bytes.fetch_add(n_bytes, Ordering::Relaxed);
                        file_progress.inc(1);
                        break;
                    }
                    Err(err) => {
                        log::error!("Error processing {:?}: {}", path, err);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        if let Ok(ref mut error) = error.try_lock() {
                            **error = Some(format!("{err:?} encounted while processing {path:?}"));
                        }
                        if retries >= max_retries {
                            early_exit.store(true, Ordering::Relaxed);
                            if let Ok(ref mut error) = error.try_lock() {
                                **error =
                                    Some(format!("{err:?} encounted while processing {path:?}"));
                            }
                            break;
                        } else {
                            log::warn!("Retrying {:?}", path);
                            if let Some(progress) = &progress {
                                progress.reset();
                            }
                            retries += 1;
                        }
                    }
                };
            }
        });

        Ok(())
    }

    pub(crate) fn done(&self) -> bool {
        (self.pool.active_count() == 0 && self.pool.queued_count() == 0)
            || self.pool.panic_count() > 0
            || self.early_exit.load(Ordering::Relaxed)
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.early_exit.load(Ordering::Relaxed)
    }

    pub(crate) fn join(&self) -> Result<()> {
        self.pool.join();

        if self.early_exit.load(Ordering::Relaxed) || self.pool.panic_count() > 0 {
            self.file_progress.finish_and_clear();
            if let Ok(ref error) = self.error.try_lock() {
                if let Some(ref err) = **error {
                    bail!("{err}");
                }
            }
            bail!("Thread worker(s) finished with errors");
        } else {
            self.file_progress.finish();
        }

        let error_count = self.error_count.load(Ordering::Relaxed);
        if error_count > 0 {
            log::warn!(
                "Encountered {} recoverable error(s) during processing",
                error_count
            );
            if let Ok(ref error) = self.error.try_lock() {
                if let Some(ref err) = **error {
                    log::warn!("Last error encountered: {err:.180}...");
                }
            }
        }

        log::info!(
            "Processed {} JSON lines in {}",
            self.total_lines
                .load(Ordering::Relaxed)
                .separate_with_commas(),
            format_duration(Duration::from_secs(self.start.elapsed().as_secs()))
        );

        Ok(())
    }
}

pub(crate) fn parse_size_default_to_gb(src: &str) -> Result<u64, parse_size::Error> {
    let mut has_unit = false;
    for c in src.chars() {
        if c.is_alphabetic() {
            has_unit = true;
            break;
        }
    }
    if has_unit {
        parse_size(src)
    } else {
        parse_size(format!("{src}GiB"))
    }
}

pub(crate) fn expand_dirs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    for path in paths {
        if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            let mut num_hits = 0;
            for entry in glob(&format!("{}/**/*.json*.gz", path_str))? {
                files.push(entry?.to_path_buf());
                num_hits += 1;
            }
            if num_hits == 0 {
                bail!("No JSON Gz files found in '{}'", path_str);
            }
        } else {
            files.push(path.clone());
        }
    }

    Ok(files)
}
