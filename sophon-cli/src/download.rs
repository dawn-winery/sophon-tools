use std::{path::PathBuf, time::Duration};

use clap::Args;
use sophon_lib::{
    GameEdition,
    api::{get_game_branches_info, get_game_download_sophon_info},
    reqwest::{self},
};

use super::GameCommon;
use crate::{CustomPackageInfo, DownloadParameters, StatusFormat};

#[derive(Debug, Args)]
/// Download the game
pub struct DownloadArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Omit to use latest
    #[arg(short, long)]
    version: Option<String>,
    /// Whether to use the preload
    #[arg(short, long)]
    preload: bool,

    /// Assemble files in-place in the game folder, without making temporary files in cache dir
    #[arg(short, long)]
    inplace: bool,

    #[command(flatten)]
    extra: DownloadParameters,

    #[command(flatten)]
    custom_package_info: CustomPackageInfo,
}

impl DownloadArgs {
    pub fn download(
        self,
        edition: GameEdition,
        temp_dir: PathBuf,
        threads: usize,
    ) -> Result<(), String> {
        let components = self
            .game
            .component
            .unwrap_or_else(|| vec!["game".to_owned()]);
        let match_extra_components = components.iter().any(|c| c == "extra-components");
        // doing this conversion because the blocking client doesn't have these options
        let client = reqwest::blocking::ClientBuilder::from(
            reqwest::ClientBuilder::new()
                .http2_adaptive_window(true)
                .http2_keep_alive_while_idle(true)
                .timeout(Duration::from_secs(30)),
        )
        .build()
        .expect("Client config should be valid");

        let output_format = self
            .extra
            .status_format
            .unwrap_or_else(StatusFormat::select_default);

        let output = output_format.into_stateful();

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
                        .get_packages_by_id_or_biz(
                            &self.game.game,
                            self.version.as_deref(),
                            self.preload,
                        )
                        .next()
                        .expect("Failed to find game branch")
                } else {
                    branches
                        .get_package_by_id_or_biz_latest(&self.game.game, self.preload)
                        .expect("Failed to find game")
                }
                .clone()
            };
        let mut downloads_info = get_game_download_sophon_info(&client, &package_info, &edition)
            .expect("Failed to get download info");

        downloads_info.manifests.retain(|download_info| {
            components.contains(&download_info.matching_field)
                || (match_extra_components
                    && !(["zh-cn", "en-us", "ja-jp", "ko-kr"]
                        .contains(&download_info.matching_field.as_str())))
        });

        output.dl_info(&downloads_info)?;

        for download_info in downloads_info.manifests {
            let total_download = download_info
                .stats
                .compressed_size
                .parse::<u64>()
                .expect("API should have valid integer");

            let matching_field = download_info.matching_field.clone();

            let updater = output
                .clone()
                .updater_download(&matching_field, total_download);

            let mut downloader = sophon_lib::installer::SophonInstaller::new(
                client.clone(),
                &download_info,
                &temp_dir,
            )
            .expect("Failed to construct downloader")
            .with_free_space_check(!self.extra.skip_free_space_check);
            downloader.inplace = self.inplace;
            downloader.chunks_in_mem = self.extra.chunk_buffer_memory;
            downloader.chunks_queue_data_limit = self.extra.memory_buffer_limit;

            let res = if !self.extra.preload_pretend {
                downloader.install(&self.game.game_dir, threads, Box::new(updater))
            } else {
                downloader.pre_download(threads, Box::new(updater))
            };
            if let Err(why) = res {
                output.abort_msg(&format!(
                    "Failed to download component `{}`: {why:?}",
                    download_info.matching_field
                ));
            } else {
                output.print_msg(&format!(
                    "Finished downloading component `{}`",
                    download_info.matching_field
                ));
            }
        }

        Ok(())
    }
}
