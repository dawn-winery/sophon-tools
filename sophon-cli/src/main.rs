use std::{
    io::{Write, stdout},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use clap::{Args, Parser, Subcommand, ValueEnum, ValueHint};
use indicatif::{ProgressBar, ProgressStyle};
use protobuf::MessageDyn;
use protobuf_json_mapping::PrintOptions;
use serde::Serialize;
use sophon_lib::{
    GameEdition,
    api::{
        get_download_manifest, get_download_manifest_raw, get_game_branches_info,
        get_game_download_sophon_info,
    },
    reqwest::{self, blocking::Client},
};

mod pretty_print;
use pretty_print::PrettyPrint;
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt,
};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Game edition, global or china
    #[arg(short, long, default_value = "global")]
    edition: String,

    /// Cache directory
    #[arg(short, long, default_value_os_t = std::env::home_dir().unwrap().join(".cache/sophon-tools"), value_hint = ValueHint::DirPath)]
    cache_dir: PathBuf,

    /// Thread limit for the commands that use multiple threads
    #[arg(short, long, default_value_t = 2)]
    threads: usize,

    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Download the game
    Download {
        #[command(flatten)]
        game: GameCommon,
        /// Omit to use latest
        #[arg(short, long)]
        version: Option<String>,
        /// Whether to use the preload
        #[arg(short, long)]
        preload: bool,

        /// Assemble files in-place in the game folder, without making temporary files in cache dir
        /// TODO: not implemented
        #[arg(long)]
        inplace: bool,

        #[command(flatten)]
        extra: DownloadParameters,
    },

    /// Update the game from one version to anotehr
    Update {
        #[command(flatten)]
        game: GameCommon,
        /// Currently installed version to update from
        #[arg(long)]
        from: String,
        /// Omit to use latest
        #[arg(long)]
        to: Option<String>,

        #[command(flatten)]
        extra: DownloadParameters,
    },

    /// Check and repair game files
    Repair {
        #[command(flatten)]
        game: GameCommon,
        /// Omit to use latest
        #[arg(short, long)]
        version: Option<String>,

        /// Don't actually repair, only check and report broken files
        #[arg(short, long)]
        dry_run: bool,
    },

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
    /// Enable memory buffering: don't store chunk files on disk, but only pass through memory.
    #[arg(long)]
    chunk_buffer_memory: bool,
}

#[derive(Debug, Subcommand)]
enum DumpTarget {
    GameScanInfo(GameScanInfoDumpArgs),
    GameBranches(GameScanInfoDumpArgs),
    PackageInfo(DownloadInfoDumpArgs),
    DownloadInfo(DownloadInfoDumpArgs),
    DownloadManifest {
        /// Game codename (biz) or id
        game: String,
        /// Game version, will pick latest if not specified
        version: Option<String>,
        /// Whether to search for preload
        #[arg(short, long)]
        preload: bool,
        /// Matching field of part of the game
        #[arg(short, long, default_value = "game")]
        matching_field: String,
    },
    PatchInfo,
    PatchManifest,
}

#[derive(Debug, Args)]
struct GameScanInfoDumpArgs {
    /// Game id
    game: Option<String>,
    /// Game version, will print all if omitted
    version: Option<String>,
    /// Only dump latest version
    #[arg(short, long)]
    latest: bool,
}

#[derive(Debug, Args)]
struct DownloadInfoDumpArgs {
    /// Game codename (biz) or id
    ///
    /// Only one will be dumped, specify id if multiple branches have the same codename
    game: String,
    /// Game version, will pick latest if not specified
    version: Option<String>,
    /// Whether to search for preload
    #[arg(short, long)]
    preload: bool,
    /// Only use latest version,
    #[arg(short, long)]
    latest: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DumpFormat {
    /// Raw response from the API
    ///
    /// Will dump RAW BYTES if the value is a protobuf message
    Raw,
    /// Parse and use rust's debug print
    Debug,
    /// Parse and use rust's indented debug print
    DebugPretty,
    /// Parse and re-serialize into json in a compact format
    Json,
    /// Parse and re-serialize into json in an indented format
    JsonPretty,
    /// Parse and print in a pretty human-readable format
    Pretty,
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

fn main() {
    init_tracing();

    let cli_args = Cli::parse();
    let edition = GameEdition::from_str(&cli_args.edition).unwrap();

    // TODO? custom error type
    let action_result = match cli_args.action {
        Action::Dump { format, target } => dump_api_data(
            edition,
            format.unwrap_or_else(|| {
                if is_piped() {
                    DumpFormat::Json
                } else {
                    DumpFormat::Pretty
                }
            }),
            target,
        ),
        Action::Download {
            game,
            version,
            preload,
            inplace,
            extra,
        } => download(
            edition,
            game,
            cli_args.cache_dir,
            version,
            preload,
            cli_args.threads,
            inplace,
            extra,
        ),
        _ => todo!(),
    };

    if let Err(err) = action_result {
        eprintln!("{err}");
        std::process::exit(1)
    }
}

fn download(
    edition: GameEdition,
    mut game_common: GameCommon,
    temp_dir: PathBuf,
    version: Option<String>,
    preload: bool,
    threads: usize,
    inplace: bool,
    extra: DownloadParameters,
) -> Result<(), String> {
    let components = game_common
        .component
        .unwrap_or_else(|| vec!["game".to_owned()]);
    // doing this conversion because the blocking client doesn't have these options
    let client = Into::<reqwest::blocking::ClientBuilder>::into(
        reqwest::ClientBuilder::new()
            .http2_adaptive_window(true)
            .http2_keep_alive_while_idle(true),
    )
    .build()
    .unwrap();
    let branches = get_game_branches_info(&client, &edition).expect("Failed to get game branches");
    let package_info = if version.is_some() {
        branches
            .get_packages_by_id_or_biz(&game_common.game, version.as_deref(), preload)
            .next()
            .expect("Failed to find game branch")
    } else {
        branches
            .get_package_by_id_or_biz_latest(&game_common.game, preload)
            .expect("Failed to find game")
    };
    let downloads_info = get_game_download_sophon_info(&client, package_info, &edition)
        .expect("Failed to get download info");

    if !dialoguer::Confirm::new()
        .with_prompt("Proceed with download?")
        .interact()
        .unwrap()
    {
        std::process::exit(1)
    }

    for download_info in downloads_info
        .manifests
        .iter()
        .filter(|download_info| components.contains(&download_info.matching_field))
    {
        let total_download = download_info.stats.compressed_size.parse::<u64>().unwrap();
        let download_style = 
                ProgressStyle::default_bar()
                .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap();
        let file_check_style = 
                ProgressStyle::default_bar()
                .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} {percent}%")
                .unwrap();

        let progress_bar = ProgressBar::new(total_download).with_style(download_style.clone());
        progress_bar.enable_steady_tick(Duration::from_secs_f32(0.25));

        let matching_field = download_info.matching_field.clone();

        let mut downloader =
            sophon_lib::installer::SophonInstaller::new(client.clone(), download_info, &temp_dir)
                .expect("Failed to construct downloader")
                .with_free_space_check(!extra.skip_free_space_check);
        downloader.inplace = inplace;
        downloader.chunks_in_mem = extra.chunk_buffer_memory;
        downloader.chunks_queue_data_limit = extra.memory_buffer_limit;
        if let Err(why) = downloader.install(&game_common.game_dir, threads, |msg| match msg {
            sophon_lib::installer::Update::DownloadingProgressBytes {
                downloaded_bytes,
                ..
            } => {
                progress_bar.set_position(downloaded_bytes);
                #[cfg(feature = "tracy")]
                {
                    let rate = progress_bar.per_sec();
                    tracing_tracy::client::plot!("Downloading speed", rate);
                }
            }
            sophon_lib::installer::Update::CheckingFiles{total_files} => {
                progress_bar.set_message("Checking existing files");
                progress_bar.set_style(file_check_style.clone());
                progress_bar.set_length(total_files);
                progress_bar.set_position(0);
            }
            sophon_lib::installer::Update::CheckingFilesProgress { passed, total } => {
                progress_bar.set_position(passed);
                if passed == total {
                    progress_bar.finish_with_message("All files are already dowloaded");
                }
            }
            sophon_lib::installer::Update::DownloadingStarted{location, total_bytes, ..} => {
                progress_bar.set_message(format!("Downloading to {}", location.display()));
                progress_bar.set_style(download_style.clone());
                progress_bar.set_length(total_bytes);
                progress_bar.set_position(0);
                progress_bar.reset_elapsed();
            }
            sophon_lib::installer::Update::CheckingFreeSpace(path) => {
                progress_bar.set_message(format!("Checking free space at {}", path.display()))
            }
            sophon_lib::installer::Update::DownloadingFinished => progress_bar
                .finish_with_message(format!("Finished downloading component {}", matching_field)),
            _ => {}
        }) {
            progress_bar.abandon_with_message(format!(
                "Failed to download component {}: {why:?}",
                download_info.matching_field
            ));
        }
    }

    Ok(())
}

fn dump_api_data(
    edition: GameEdition,
    format: DumpFormat,
    target: DumpTarget,
) -> Result<(), String> {
    let client = sophon_lib::reqwest::blocking::Client::new();
    match target {
        DumpTarget::GameScanInfo(args) => dump_game_scan_info(&client, edition, format, args),
        DumpTarget::GameBranches(args) => dump_game_branches(&client, edition, format, args),
        DumpTarget::PackageInfo(args) => dump_package_info(&client, edition, format, args),
        DumpTarget::DownloadInfo(args) => dump_download_info(&client, edition, format, args),

        DumpTarget::DownloadManifest {
            game,
            version,
            preload,
            matching_field,
        } => dump_download_manifest(
            &client,
            edition,
            format,
            game,
            version,
            matching_field,
            preload,
        ),

        _ => todo!(),
    }
}

fn dump_game_scan_info(
    client: &Client,
    edition: GameEdition,
    format: DumpFormat,
    GameScanInfoDumpArgs {
        game,
        version,
        latest,
    }: GameScanInfoDumpArgs,
) -> Result<(), String> {
    if matches!(format, DumpFormat::Raw) {
        println!(
            "{}",
            sophon_lib::api::get_game_scan_info_raw(client, &edition).unwrap()
        );
        return Ok(());
    }

    let game_scan_info = sophon_lib::api::get_game_scan_info(client, &edition).unwrap();

    let Some(game_id) = game else {
        dump_value_formatted(&game_scan_info, format);
        return Ok(());
    };

    let Some(game_filtered) = game_scan_info
        .game_scan_info
        .iter()
        .find(|info| info.game_id == game_id)
    else {
        return Err(format!(
            "Failed to find scan info for game id `{}`",
            game_id
        ));
    };

    if latest {
        if let Some(game_latest) = game_filtered
            .game_exe_list
            .iter()
            .max_by_key(|hash| &hash.version)
        {
            dump_value_formatted(game_latest, format);
            Ok(())
        } else {
            Err("No exe versions, this is unexpected".to_string())
        }
    } else if let Some(target_version) = version {
        if let Some(version_filtered) = game_filtered
            .game_exe_list
            .iter()
            .find(|info| info.version == target_version)
        {
            dump_value_formatted(version_filtered, format);
            Ok(())
        } else {
            Err(format!("Version {target_version} not found"))
        }
    } else {
        dump_value_formatted(game_filtered, format);
        Ok(())
    }
}

fn dump_game_branches(
    client: &Client,
    edition: GameEdition,
    format: DumpFormat,
    GameScanInfoDumpArgs {
        game,
        version,
        latest,
    }: GameScanInfoDumpArgs,
) -> Result<(), String> {
    if matches!(format, DumpFormat::Raw) {
        println!(
            "{}",
            sophon_lib::api::get_game_branches_info_raw(client, &edition).unwrap()
        );
        return Ok(());
    }

    let game_branches = sophon_lib::api::get_game_branches_info(client, &edition).unwrap();

    let Some(game_id_or_biz) = game else {
        dump_value_formatted(&game_branches, format);
        return Ok(());
    };

    if latest {
        if let Some(latest_branch) =
            game_branches.get_game_branch_by_id_or_biz_latest(&game_id_or_biz)
        {
            dump_value_formatted(latest_branch, format);
            return Ok(());
        }
    } else {
        let mut filtered_branches =
            game_branches.get_game_branches_by_id_or_biz(&game_id_or_biz, version.as_deref());
        if let Some(first_item) = filtered_branches.next() {
            dump_value_formatted(first_item, format);
            for branch in filtered_branches {
                if matches!(format, DumpFormat::Pretty) {
                    print!("\n\n");
                }
                dump_value_formatted(branch, format);
            }
            return Ok(());
        }
    }
    Err("Unable to find game branches with specified query".to_string())
}

fn dump_package_info(
    client: &Client,
    edition: GameEdition,
    format: DumpFormat,
    DownloadInfoDumpArgs {
        game: game_id_or_biz,
        version,
        preload,
        latest,
    }: DownloadInfoDumpArgs,
) -> Result<(), String> {
    if matches!(format, DumpFormat::Raw) {
        return Err(
            "Unable to filter and extract package information with raw formatting".to_string(),
        );
    }
    let game_branches = sophon_lib::api::get_game_branches_info(client, &edition).unwrap();

    if latest {
        if let Some(latest_branch) =
            game_branches.get_package_by_id_or_biz_latest(&game_id_or_biz, preload)
        {
            dump_value_formatted(latest_branch, format);
        }
        return Ok(());
    } else {
        let mut filtered_branches =
            game_branches.get_packages_by_id_or_biz(&game_id_or_biz, version.as_deref(), preload);
        if let Some(first_item) = filtered_branches.next() {
            dump_value_formatted(first_item, format);
            for branch in filtered_branches {
                if matches!(format, DumpFormat::Pretty) {
                    print!("\n\n");
                }
                dump_value_formatted(branch, format);
            }
            return Ok(());
        }
    }

    Err("Unable to find packages with specified query".to_string())
}

fn dump_download_info(
    client: &Client,
    edition: GameEdition,
    format: DumpFormat,
    DownloadInfoDumpArgs {
        game,
        version,
        preload,
        // TODO
        latest,
    }: DownloadInfoDumpArgs,
) -> Result<(), String> {
    let game_branches = sophon_lib::api::get_game_branches_info(client, &edition).unwrap();

    let Some(package) = game_branches
        .get_packages_by_id_or_biz(&game, version.as_deref(), preload)
        .next()
    else {
        return Err("Unable to find package with specified query".to_string());
    };

    if matches!(format, DumpFormat::Raw) {
        println!(
            "{}",
            sophon_lib::api::get_game_download_sophon_info_raw(client, package, &edition).unwrap()
        );
        return Ok(());
    }

    let download_info =
        sophon_lib::api::get_game_download_sophon_info(client, package, &edition).unwrap();

    dump_value_formatted(&download_info, format);

    Ok(())
}

fn dump_download_manifest(
    client: &Client,
    edition: GameEdition,
    format: DumpFormat,
    game: String,
    version: Option<String>,
    matching_field: String,
    preload: bool,
) -> Result<(), String> {
    let game_branches = sophon_lib::api::get_game_branches_info(client, &edition).unwrap();

    let Some(package) = game_branches
        .get_packages_by_id_or_biz(&game, version.as_deref(), preload)
        .next()
    else {
        return Err("Unable to find package with specified query".to_string());
    };

    let downloads =
        sophon_lib::api::get_game_download_sophon_info(client, package, &edition).unwrap();
    let Some(download_info) = downloads
        .manifests
        .iter()
        .find(|download_info| download_info.matching_field == matching_field)
    else {
        return Err(format!(
            "Download info with `matching_field` == `{matching_field}` not found"
        ));
    };

    if matches!(format, DumpFormat::Raw) {
        let data = get_download_manifest_raw(client, download_info).unwrap();
        let mut output = stdout();
        output.write_all(&data).unwrap();
        return Ok(());
    }

    let download_manifest = get_download_manifest(client, download_info).unwrap();

    dump_protobuf_formatted(&download_manifest, format);

    Ok(())
}

// Helpers for outputting data in all the supported formats except raw

fn dump_value_formatted<T>(value: &T, format: DumpFormat)
where
    T: core::fmt::Debug,
    T: PrettyPrint,
    T: Serialize,
{
    match format {
        DumpFormat::Debug => println!("{value:?}"),
        DumpFormat::DebugPretty => println!("{value:#?}"),
        DumpFormat::Json => {
            let serialized = serde_json::to_string(&value).expect("Failed to serialize value");
            println!("{}", serialized)
        }
        DumpFormat::JsonPretty => {
            let serialized =
                serde_json::to_string_pretty(&value).expect("Failed to serialize value");
            println!("{}", serialized)
        }
        DumpFormat::Pretty => {
            value.pretty_print();
        }
        // I don't really like this unreachable, but removing it would probably need a refactor.
        //
        // The blocker is that there's no generic way to get the raw representation, it's a
        // different function call. Maybe making some ApiType trait that has a raw and normal
        // getter types would work. Add filtering/subquerying to that too for extra code
        // deduplication. Although at that point I have a hunch that it blows up into being more
        // complicated to maintain that what is here now.
        DumpFormat::Raw => unreachable!("Handled earlier in code"),
    }
}

fn dump_protobuf_formatted<T>(value: &T, format: DumpFormat)
where
    T: core::fmt::Debug,
    T: PrettyPrint,
    T: MessageDyn,
{
    match format {
        DumpFormat::Debug => println!("{value:?}"),
        DumpFormat::DebugPretty => println!("{value:#?}"),
        DumpFormat::JsonPretty | DumpFormat::Json => {
            let options = PrintOptions {
                enum_values_int: false,
                proto_field_name: true,
                always_output_default_values: true,
                _future_options: (),
            };
            let mut serialized =
                protobuf_json_mapping::print_to_string_with_options(value, &options)
                    .expect("Failed to serialize value");
            if matches!(format, DumpFormat::JsonPretty) {
                let deserialized: serde_json::Value =
                    serde_json::from_str(&serialized).expect("Failed to deserialize produced json");
                serialized = serde_json::to_string_pretty(&deserialized)
                    .expect("Failed to re-serialize value");
            }
            println!("{}", serialized)
        }
        DumpFormat::Pretty => {
            value.pretty_print();
        }
        DumpFormat::Raw => unreachable!("Handled earlier in code"),
    }
}
