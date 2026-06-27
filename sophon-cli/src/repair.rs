use std::{path::PathBuf, time::Duration};

use clap::Args;
use sophon_lib::{
    GameEdition,
    api::{get_game_branches_info, get_game_download_sophon_info},
    reqwest,
};

use super::{DownloadParameters, GameCommon};
use crate::{CustomPackageInfo, status_format::StatusFormat};

#[derive(Debug, Args)]
/// Check and repair game files
pub struct RepairArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Omit to use latest
    #[arg(short, long)]
    version: Option<String>,

    /// Assemble files in-place in the game folder, without making temporary files in cache dir
    #[arg(short, long)]
    inplace: bool,
    /// Don't actually repair, only check and report broken files
    #[arg(short, long)]
    dry_run: bool,

    #[command(flatten)]
    extra: DownloadParameters,

    #[command(flatten)]
    custom_package_info: CustomPackageInfo,
}

impl RepairArgs {
    pub fn repair(
        mut self,
        edition: GameEdition,
        temp_dir: PathBuf,
        threads: usize,
    ) -> Result<(), String> {
        let output_format = self
            .extra
            .status_format
            .unwrap_or_else(StatusFormat::select_default);

        let output = output_format.into_stateful();

        if let Some(game_ver) = &mut self.version
            && game_ver == "auto"
            && let Some(auto_ver) =
                super::update::autodetect_game_ver(&self.game.game_dir, &self.game.game, &edition)
                    .inspect_err(|err| {
                        output.print_msg(&format!("Error autodetecting game version: {err}"));
                    })
                    .unwrap_or(None)
        {
            *game_ver = auto_ver
        }

        let components = self
            .game
            .component
            .unwrap_or_else(|| vec!["game".to_owned()]);
        // doing this conversion because the blocking client doesn't have these options
        let client = reqwest::blocking::ClientBuilder::from(
            reqwest::ClientBuilder::new()
                .http2_adaptive_window(true)
                .http2_keep_alive_while_idle(true)
                .timeout(Duration::from_secs(30)),
        )
        .build()
        .expect("Client configuration should be valid");

        let package_info =
            if let Some(adhoc_package_info) = self.custom_package_info.assemble_adhoc() {
                output.print_msg("Using provided ad-hoc package info");
                adhoc_package_info
            } else {
                output.print_msg("Fetching download information...");
                let branches =
                    get_game_branches_info(&client, &edition).expect("Failed to get game branches");
                if self.version.is_some() {
                    branches
                        .get_packages_by_id_or_biz(&self.game.game, self.version.as_deref(), false)
                        .next()
                        .expect("Failed to find game branch")
                } else {
                    branches
                        .get_package_by_id_or_biz_latest(&self.game.game, false)
                        .expect("Failed to find game")
                }
                .clone()
            };

        let mut downloads_info = get_game_download_sophon_info(&client, &package_info, &edition)
            .expect("Failed to get download info");

        downloads_info
            .manifests
            .retain(|download_info| components.contains(&download_info.matching_field));

        output.repair_info(&downloads_info)?;

        for download_info in downloads_info
            .manifests
            .iter()
            .filter(|download_info| components.contains(&download_info.matching_field))
        {
            let total_download = download_info
                .stats
                .compressed_size
                .parse::<u64>()
                .expect("API must have valid integer");

            let matching_field = download_info.matching_field.clone();

            let updater = output
                .clone()
                .updater_repair(&matching_field, total_download);

            let mut downloader = sophon_lib::installer::SophonInstaller::new(
                client.clone(),
                download_info,
                &temp_dir,
            )
            .expect("Failed to construct downloader")
            .with_free_space_check(!self.extra.skip_free_space_check);
            downloader.inplace = self.inplace;
            downloader.chunks_in_mem = self.extra.chunk_buffer_memory;
            downloader.chunks_queue_data_limit = self.extra.memory_buffer_limit;
            downloader.skip_download_repair = self.dry_run;
            downloader.mode_repair = true;

            if let Err(why) = downloader.install(&self.game.game_dir, threads, Box::new(updater)) {
                output.abort_msg(&format!(
                    "Failed to repair component `{}`: {why:?}",
                    download_info.matching_field
                ));
            } else if !output.is_finished() {
                output.print_msg(&format!(
                    "Component `{}`: not all files passed the check",
                    download_info.matching_field
                ));
            }
        }

        Ok(())
    }
}
