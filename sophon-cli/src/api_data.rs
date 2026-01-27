use std::io::{Write, stdout};

use clap::{Args, ValueEnum};
use protobuf::MessageDyn;
use protobuf_json_mapping::PrintOptions;
use serde::Serialize;
use sophon_lib::{
    GameEdition,
    api::{get_download_manifest, get_download_manifest_raw},
    reqwest::blocking::Client,
};

use crate::pretty_print::PrettyPrint;

#[derive(Debug, clap::Subcommand)]
pub enum DumpTarget {
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
pub struct GameScanInfoDumpArgs {
    /// Game id
    game: Option<String>,
    /// Game version, will print all if omitted
    version: Option<String>,
    /// Only dump latest version
    #[arg(short, long)]
    latest: bool,
}

#[derive(Debug, Args)]
pub struct DownloadInfoDumpArgs {
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
pub enum DumpFormat {
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

pub fn decide_format(user_selection: Option<DumpFormat>) -> DumpFormat {
    user_selection.unwrap_or_else(|| {
        if super::is_piped() {
            DumpFormat::Json
        } else {
            DumpFormat::Pretty
        }
    })
}

impl DumpTarget {
    pub fn dump_api_data(self, edition: GameEdition, format: DumpFormat) -> Result<(), String> {
        let client = sophon_lib::reqwest::blocking::Client::new();
        match self {
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
