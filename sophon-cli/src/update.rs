use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use clap::Args;
use sophon_lib::{
    GameEdition, SophonError,
    api::{
        get_game_branches_info, get_game_configs, get_game_diffs_sophon_info, get_game_scan_info,
    },
    reqwest::{self},
    updater::SophonPatcher,
    utils::version::Version,
};

use super::{DownloadParameters, GameCommon};
use crate::{CustomPackageInfo, status_format::StatusFormat};

pub fn autodetect_game_ver(
    game_folder: &Path,
    game: &str,
    edition: &GameEdition,
) -> Result<Option<String>, SophonError> {
    let client = reqwest::blocking::Client::new();
    let game_scan_info = get_game_scan_info(&client, edition)?;
    let game_configs = get_game_configs(&client, edition)?;
    let game_id = game_configs
        .launch_configs
        .iter()
        .find(|config| config.game.biz == game)
        .map(|config| config.game.id.as_ref())
        .inspect(|game_id| {
            println!("Matched game biz to id {game_id}");
        })
        .unwrap_or(game);
    let Some(filtered) = game_scan_info
        .game_scan_info
        .into_iter()
        .find(|scan_info| scan_info.game_id == game_id)
    else {
        eprintln!("Game id `{game}` not found!");
        return Ok(None);
    };

    let Some(exe_name) = game_configs
        .launch_configs
        .iter()
        .find_map(|launch_config| {
            (launch_config.game.id == game_id).then_some(launch_config.exe_file_name.clone())
        })
    else {
        return Ok(None);
    };
    let exe_path = game_folder.join(exe_name);

    for hash in filtered.game_exe_list {
        if sophon_lib::file_md5_hash_str(&exe_path)
            .map(|generated_hash| generated_hash == hash.md5)
            .unwrap_or(false)
        {
            return Ok(Some(hash.version));
        }
    }

    Ok(None)
}

#[derive(Debug, Args)]
/// Update the game from one version to anotehr
pub struct UpdateArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Currently installed version to update from. Use value of `auto` to autodetect the installed
    /// game version
    #[arg(short, long)]
    from: String,
    /// Omit to use latest
    #[arg(short, long)]
    to: Option<String>,

    /// Whether to use the preload (just downloads all patches and blobs into the cache dir)
    #[arg(short, long)]
    preload: bool,

    #[command(flatten)]
    extra: DownloadParameters,

    #[command(flatten)]
    custom_package_info: CustomPackageInfo,
}

impl UpdateArgs {
    pub fn update(
        mut self,
        game_edition: GameEdition,
        cache_dir: PathBuf,
        thread_count: usize,
    ) -> Result<(), String> {
        let output_format = self
            .extra
            .status_format
            .unwrap_or_else(StatusFormat::select_default);

        let output = output_format.into_stateful();

        if self.from == "auto"
            && let Some(auto_ver) =
                autodetect_game_ver(&self.game.game_dir, &self.game.game, &game_edition)
                    .map_err(|e| e.to_string())
                    .inspect_err(|err| eprintln!("Error autodetecting game version: {err}"))
                    .unwrap_or(None)
        {
            output.print_msg("Autodetected {auto_ver}");
            self.from = auto_ver;
        };

        if self.from == "auto" {
            output.abort_msg("Could not autodetect game version");
            return Ok(());
        }
        if self.game.component.is_none() {
            self.game.component = Some(vec!["game".to_owned()]);
        };
        let components = self.game.component.as_ref().expect("was just set to Some");
        // doing this conversion because the blocking client doesn't have these options
        let client = reqwest::blocking::ClientBuilder::from(
            reqwest::ClientBuilder::new()
                .http2_adaptive_window(true)
                .http2_keep_alive_while_idle(true)
                .timeout(Duration::from_secs(30)),
        )
        .build()
        .expect("Must be a valid configuration");

        let package_info = if let Some(adhoc_package_info) =
            self.custom_package_info.assemble_adhoc()
        {
            output.print_msg("Using provided ad-hoc package info");
            adhoc_package_info
        } else {
            output.print_msg("Fetching update information...");
            let branches = get_game_branches_info(&client, &game_edition)
                .expect("Failed to get game branches");
            if self.to.is_some() {
                branches
                    .get_packages_by_id_or_biz(&self.game.game, self.to.as_deref(), self.preload)
                    .next()
                    .expect("Failed to find game branch")
            } else {
                branches
                    .get_package_by_id_or_biz_latest(&self.game.game, self.preload)
                    .expect("Failed to find game")
            }
            .clone()
        };

        let mut diffs_info = get_game_diffs_sophon_info(&client, &package_info, &game_edition)
            .expect("Failed to get update info");

        if diffs_info.tag == self.from {
            output.abort_msg("Attempting to update the version to itself, this is a no-op");
            return Ok(());
        }

        diffs_info
            .manifests
            .retain(|diff| components.contains(&diff.matching_field));

        output.update_info(&diffs_info)?;

        for update_manifest in diffs_info.manifests {
            let total_download = update_manifest
                .stats
                .iter()
                .find(|(k, _)| **k == self.from)
                .and_then(|(_, v)| v.compressed_size.parse::<u64>().ok())
                .expect("Failed to find/parse download size");

            let matching_field = update_manifest.matching_field.clone();

            let updater = output
                .clone()
                .updater_update(total_download, &matching_field);

            let mut patcher =
                SophonPatcher::new(client.clone(), &update_manifest, &cache_dir, None)
                    .expect("Failed to construct updater")
                    .with_free_space_check(!self.extra.skip_free_space_check);
            patcher.patches_in_memory = self.extra.chunk_buffer_memory;
            patcher.patch_queue_mem_limit = self.extra.memory_buffer_limit;
            let res = if !self.extra.preload_pretend {
                patcher.update(
                    &self.game.game_dir,
                    Version::from_str(&self.from).expect("API must have valid version string"),
                    thread_count,
                    Box::new(updater),
                )
            } else {
                patcher.pre_download(
                    Version::from_str(&self.from).expect("API must have valid version string"),
                    thread_count,
                    Box::new(updater),
                )
            };

            if let Err(why) = res {
                output.abort_msg(&format!(
                    "Failed to update component `{}`: {why:?}",
                    update_manifest.matching_field
                ));
            } else {
                output.print_msg(&format!(
                    "Done updating component `{}`",
                    update_manifest.matching_field,
                ));
            }
        }

        Ok(())
    }
}
