use anyhow::Result;
use structopt::StructOpt;

mod cmd;
pub mod s3;
pub mod io;
pub mod ngrams;
pub mod progress;
pub mod tokens;
pub mod util;

#[derive(Debug, StructOpt)]
#[structopt(version = option_env!("BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))]
#[structopt(
    name = "wimbd",
    about = "What's in my big data?",
    setting = structopt::clap::AppSettings::ColoredHelp,
)]
struct Opt {
    #[structopt(subcommand)]
    cmd: WimbdCmd,
}

#[derive(Debug, StructOpt)]
enum WimbdCmd {
    /// Find the top-k ngrams in a dataset of compressed JSON lines files using a counting Bloom
    /// filter.
    ///
    /// Work is parallelized over files.
    ///
    /// EXAMPLES
    ///
    /// Find the top 20 3-grams in a file:
    ///
    /// > wimbd topk c4-train.01011-of-01024.json.gz --ngram=3 --topk=20 --seed=42 --size=50GiB
    ///
    /// Find the top 20 100-grams in a file:
    ///
    /// > wimbd topk c4-train.01011-of-01024.json.gz --ngram=3 --topk=20 --seed=42 --size=50GiB
    ///
    /// You can also pass directories instead of files, in which case files will be found by
    /// globbing for '**/*.json.gz' within each directory.
    ///
    /// ACCURACY
    ///
    /// In general you should set '--size' to however many free gigabytes of RAM you have available, minus some buffer room.
    /// This minimizes the probability of incorrect counts and false positives in the top-k.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Topk(cmd::topk::Opt),

    /// Like 'topk' but for finding the least common ngrams.
    ///
    /// Work is parallelized over files.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Botk(cmd::botk::Opt),

    /// Get exact counts for given search strings. Note that the search strings will be tokenized
    /// and the search will be done over tokens instead of searching for those substrings directly.
    ///
    /// If you want to count occurrences of a regex pattern instead, use the 'search' command.
    ///
    /// Work is parallelized over files.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Count(cmd::count::Opt),

    /// Get exact counts for matches of given regex patterns.
    ///
    /// Work is parallelized over files.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Search(cmd::search::Opt),

    /// Collect summary statistics about a dataset.
    ///
    /// Work is parallelized over files.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Stats(cmd::stats::Opt),

    /// Estimate the number of unique ngrams in a dataset using a Bloom filter.
    ///
    /// Work is parallelized over files.
    #[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
    Unique(cmd::unique::Opt),
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    simple_logger::init_with_level(log::Level::Info)?;

    let result = match opt.cmd {
        WimbdCmd::Topk(opt) => cmd::topk::main(opt),
        WimbdCmd::Count(opt) => cmd::count::main(opt),
        WimbdCmd::Search(opt) => cmd::search::main(opt),
        WimbdCmd::Stats(opt) => cmd::stats::main(opt),
        WimbdCmd::Botk(opt) => cmd::botk::main(opt),
        WimbdCmd::Unique(opt) => cmd::unique::main(opt),
    };

    if let Err(err) = result {
        log::error!("{}", err);
        std::process::exit(1);
    }

    Ok(())
}
