use std::{path::PathBuf, time::Duration};

use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use sophon_lib::{
    GameEdition,
    api::{get_game_branches_info, get_game_download_sophon_info},
    reqwest,
};

use super::{DownloadParameters, GameCommon};

#[derive(Debug, Args)]
/// Check and repair game files
pub struct RepairArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Omit to use latest
    #[arg(short, long)]
    version: Option<String>,

    /// Assemble files in-place in the game folder, without making temporary files in cache dir
    #[arg(long)]
    inplace: bool,
    /// Don't actually repair, only check and report broken files
    #[arg(short, long)]
    dry_run: bool,

    #[command(flatten)]
    extra: DownloadParameters,
}

impl RepairArgs {
    fn new_updater(
        progress_bar: &ProgressBar,
        download_style: &ProgressStyle,
        file_check_style: &ProgressStyle,
        matching_field: &str,
    ) -> impl Fn(sophon_lib::installer::Update) + Clone + Send {
        move |msg| match msg {
            sophon_lib::installer::Update::DownloadingProgressBytes {
                downloaded_bytes, ..
            } => {
                progress_bar.set_position(downloaded_bytes);
                #[cfg(feature = "tracy")]
                {
                    let rate = progress_bar.per_sec();
                    tracing_tracy::client::plot!("Downloading speed", rate);
                }
            }
            sophon_lib::installer::Update::CheckingFiles { total_files } => {
                progress_bar.set_message("Checking files");
                progress_bar.set_style(file_check_style.clone());
                progress_bar.set_length(total_files);
                progress_bar.set_position(0);
            }
            sophon_lib::installer::Update::CheckingFilesProgress { passed, total } => {
                progress_bar.set_position(passed);
                if passed == total {
                    progress_bar.finish_with_message("All files passed the check");
                }
            }
            sophon_lib::installer::Update::DownloadingStarted {
                location,
                total_bytes,
                ..
            } => {
                progress_bar.set_message(format!("Repairing files at {}", location.display()));
                progress_bar.set_style(download_style.clone());
                progress_bar.set_length(total_bytes);
                progress_bar.set_position(0);
                progress_bar.reset_elapsed();
            }
            sophon_lib::installer::Update::CheckingFreeSpace(path) => {
                progress_bar.set_message(format!("Checking free space at {}", path.display()))
            }
            sophon_lib::installer::Update::DownloadingFinished => progress_bar
                .finish_with_message(format!("Finished repairing component {}", matching_field)),
            _ => {}
        }
    }

    pub fn repair(
        self,
        edition: GameEdition,
        temp_dir: PathBuf,
        threads: usize,
    ) -> Result<(), String> {
        let components = self
            .game
            .component
            .unwrap_or_else(|| vec!["game".to_owned()]);
        // doing this conversion because the blocking client doesn't have these options
        let client = reqwest::blocking::ClientBuilder::from(
            reqwest::ClientBuilder::new()
                .http2_adaptive_window(true)
                .http2_keep_alive_while_idle(true),
        )
        .build()
        .unwrap();

        let branches =
            get_game_branches_info(&client, &edition).expect("Failed to get game branches");
        let package_info = if self.version.is_some() {
            branches
                .get_packages_by_id_or_biz(&self.game.game, self.version.as_deref(), false)
                .next()
                .expect("Failed to find game branch")
        } else {
            branches
                .get_package_by_id_or_biz_latest(&self.game.game, false)
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
            let file_check_style = ProgressStyle::default_bar()
                .template(
                    "{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} {percent}%",
                )
                .unwrap();

            let progress_bar = ProgressBar::new(total_download).with_style(download_style.clone());
            progress_bar.enable_steady_tick(Duration::from_secs_f32(0.25));

            let matching_field = download_info.matching_field.clone();

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
            if let Err(why) = downloader.install(
                &self.game.game_dir,
                threads,
                Self::new_updater(
                    &progress_bar,
                    &download_style,
                    &file_check_style,
                    &matching_field,
                ),
            ) {
                progress_bar.abandon_with_message(format!(
                    "Failed to download component {}: {why:?}",
                    download_info.matching_field
                ));
            }
        }

        Ok(())
    }
}
