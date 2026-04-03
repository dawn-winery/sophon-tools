use std::{path::PathBuf, str::FromStr};

use clap::{Args, Parser, Subcommand, ValueHint};
use sophon_lib::GameEdition;
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt,
};

mod api_data;
mod download;
mod repair;
mod update;

use api_data::{DumpFormat, DumpTarget};
use download::DownloadArgs;
use repair::RepairArgs;
use update::UpdateArgs;

mod pretty_print;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Game edition, global or china
    #[arg(short = 'E', long, default_value = "global")]
    edition: String,

    /// Cache directory
    #[arg(short = 'C', long, default_value_os_t = std::env::home_dir().unwrap().join(".cache/sophon-tools"), value_hint = ValueHint::DirPath)]
    cache_dir: PathBuf,

    /// Thread limit for the commands that use multiple threads
    #[arg(short = 'T', long, default_value_t = 2)]
    threads: usize,

    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Download(#[command(flatten)] DownloadArgs),

    Update(#[command(flatten)] UpdateArgs),

    Repair(#[command(flatten)] RepairArgs),

    /// Dump various API data
    Dump {
        /// Whether to output as JSON. Autodetected if omitted.
        #[arg(short, long)]
        format: Option<DumpFormat>,

        #[command(subcommand)]
        target: DumpTarget,
    },
}

#[derive(Debug, Args)]
struct GameCommon {
    /// Game codename (biz) or id
    game: String,
    /// Path to game directory
    #[arg(value_hint = ValueHint::DirPath)]
    game_dir: PathBuf,
    /// Game component(s) to check and repair, defaults to `game` if unset. Set multiple times to
    /// target multiple components.
    #[arg(short, long)]
    component: Option<Vec<String>>,
}

#[derive(Debug, Args)]
struct DownloadParameters {
    /// Skip checking for free space
    #[arg(long)]
    skip_free_space_check: bool,
    /// Set limit of how much chunk data can be buffered in the queue. Download will be
    /// throttled if the queue reaches this size.
    #[arg(long)]
    memory_buffer_limit: Option<u64>,
    /// Enable memory buffering: don't store chunk files on disk, but only pass through memory when
    /// possible.
    #[arg(long)]
    chunk_buffer_memory: bool,
    /// Pretend this is a preload, so don't install/update files, only download and store in cache
    /// the intermediates
    #[arg(long)]
    preload_pretend: bool,
}

fn is_piped() -> bool {
    atty::isnt(atty::Stream::Stdout)
}

fn init_tracing() {
    let stdout_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_filter(EnvFilter::from_default_env())
        .with_filter(filter_fn(|metadata| {
            !metadata.target().contains("rustls")
                && !metadata.target().contains("reqwest")
                && !metadata.target().contains("h2")
                && !metadata.target().contains("hyper_util")
        }));

    let registry = tracing_subscriber::registry().with(stdout_layer);

    #[cfg(feature = "tracy")]
    let registry = registry.with(
        tracing_tracy::TracyLayer::default()
            .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG)
            .with_filter(filter_fn(move |metadata| {
                !metadata.target().contains("h2") && !metadata.target().contains("hyper_util")
            })),
    );

    registry.init()
}

fn main() -> Result<(), String> {
    init_tracing();

    let cli_args = Cli::parse();
    let edition = GameEdition::from_str(&cli_args.edition).unwrap();

    // TODO? custom error type

    match cli_args.action {
        Action::Dump { format, target } => {
            target.dump_api_data(edition, api_data::decide_format(format))
        }
        Action::Download(args) => args.download(edition, cli_args.cache_dir, cli_args.threads),
        Action::Repair(args) => args.repair(edition, cli_args.cache_dir, cli_args.threads),
        Action::Update(args) => args.update(edition, cli_args.cache_dir, cli_args.threads),
    }
}
