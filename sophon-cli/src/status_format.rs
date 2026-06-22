use std::time::Duration;

use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use sophon_lib::api::schemas::{sophon_diff::SophonDiffs, sophon_manifests::SophonDownloads};

use super::ndjson_messages::NdJsonMessage;
use crate::pretty_print::PrettyPrint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StatusFormat {
    ProgressBar,
    #[value(alias = "jsonl")]
    Ndjson,
}

impl StatusFormat {
    pub fn select_default() -> Self {
        if super::is_piped() {
            StatusFormat::Ndjson
        } else {
            StatusFormat::ProgressBar
        }
    }

    #[allow(dead_code)]
    pub fn print_msg(&self, msg: &str) {
        match self {
            Self::ProgressBar => println!("{msg}"),
            Self::Ndjson => NdJsonMessage::msg(msg).print(),
        }
    }

    pub fn into_stateful(self) -> StatusFormatStateful {
        StatusFormatStateful(match self {
            Self::ProgressBar => StatusFormatStateInner::ProgressBar(ProgressBar::new(0)),
            Self::Ndjson => StatusFormatStateInner::NdJson,
        })
    }
}

/// The inner field is explicitly private to avoid access outside of this module and make sure
/// other modules use teh generalized API.
/// If you need more `updater`s - clone the updater.
#[derive(Debug, Clone)]
pub struct StatusFormatStateful(StatusFormatStateInner);

#[derive(Debug, Clone)]
enum StatusFormatStateInner {
    ProgressBar(indicatif::ProgressBar),
    NdJson,
}

impl StatusFormatStateful {
    pub fn print_msg(&self, msg: &str) {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(pb) => pb.println(msg),
            StatusFormatStateInner::NdJson => NdJsonMessage::msg(msg).print(),
        }
    }

    pub fn abort_msg(&self, msg: &str) {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(pb) => pb.abandon_with_message(msg.to_owned()),
            StatusFormatStateInner::NdJson => NdJsonMessage::fail(msg).print(),
        }
    }

    pub fn is_finished(&self) -> bool {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(pb) => pb.is_finished(),
            StatusFormatStateInner::NdJson => true,
        }
    }

    pub fn dl_info(&self, dl_info: &SophonDownloads) -> Result<(), String> {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(_) => {
                dl_info.pretty_print(0);
                println!();

                if !dialoguer::Confirm::new()
                    .with_prompt("Proceed with download?")
                    .interact()
                    .map_err(|e| e.to_string())?
                {
                    return Err("Aborted by user".to_owned());
                }

                Ok(())
            }
            StatusFormatStateInner::NdJson => {
                NdJsonMessage::DownloadInfo(dl_info).print();
                Ok(())
            }
        }
    }

    pub fn repair_info(&self, dl_info: &SophonDownloads) -> Result<(), String> {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(_) => {
                dl_info.pretty_print(0);
                println!();

                if !dialoguer::Confirm::new()
                    .with_prompt("Proceed with repair?")
                    .interact()
                    .map_err(|e| e.to_string())?
                {
                    return Err("Aborted by user".to_owned());
                }

                Ok(())
            }
            StatusFormatStateInner::NdJson => {
                NdJsonMessage::RepairInfo(dl_info).print();
                Ok(())
            }
        }
    }

    pub fn update_info(&self, upd_info: &SophonDiffs) -> Result<(), String> {
        match &self.0 {
            StatusFormatStateInner::ProgressBar(_) => {
                upd_info.pretty_print(0);
                println!();

                if !dialoguer::Confirm::new()
                    .with_prompt("Proceed with update?")
                    .interact()
                    .map_err(|e| e.to_string())?
                {
                    return Err("Aborted by user".to_owned());
                }
                Ok(())
            }
            StatusFormatStateInner::NdJson => {
                NdJsonMessage::UpdateInfo(upd_info).print();
                Ok(())
            }
        }
    }

    pub fn updater_download<'a>(
        self,
        matching_field: &'a str,
        total_download: u64,
    ) -> impl Fn(sophon_lib::installer::Update) + Clone + Send + 'a {
        let download_style =
                ProgressStyle::default_bar()
                .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("Template should be valid");
        let file_check_style = ProgressStyle::default_bar()
            .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} {percent}%")
            .expect("Template should be valid");

        if let StatusFormatStateInner::ProgressBar(pb) = &self.0 {
            pb.reset();
            pb.set_length(total_download);
            pb.set_style(download_style.clone());
            pb.enable_steady_tick(Duration::from_secs_f32(0.25));
        }

        move |msg| match &self.0 {
            StatusFormatStateInner::ProgressBar(progress_bar) => {
                Self::download_msg_progressbar(
                    progress_bar,
                    msg,
                    matching_field,
                    &download_style,
                    &file_check_style,
                );
            }
            StatusFormatStateInner::NdJson => NdJsonMessage::dlmsg(msg).print(),
        }
    }

    fn download_msg_progressbar(
        progress_bar: &ProgressBar,
        msg: sophon_lib::installer::Update,
        matching_field: &str,
        download_style: &ProgressStyle,
        file_check_style: &ProgressStyle,
    ) {
        match msg {
            sophon_lib::installer::Update::DownloadingProgressBytes {
                downloaded_bytes, ..
            } => {
                let reset_eta = progress_bar.position() == 0;
                progress_bar.set_position(downloaded_bytes);
                if reset_eta {
                    progress_bar.reset_elapsed();
                    progress_bar.reset_eta();
                }
                #[cfg(feature = "tracy")]
                {
                    let rate = progress_bar.per_sec();
                    tracing_tracy::client::plot!("Downloading speed", rate);
                }
            }
            sophon_lib::installer::Update::CheckingFiles { total_files } => {
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
            sophon_lib::installer::Update::DownloadingStarted {
                location,
                total_bytes,
                ..
            } => {
                progress_bar.set_message(format!("Downloading to {}", location.display()));
                progress_bar.set_style(download_style.clone());
                progress_bar.set_length(total_bytes);
                progress_bar.set_position(0);
                progress_bar.reset_elapsed();
                progress_bar.reset_eta();
            }
            sophon_lib::installer::Update::CheckingFreeSpace(path) => {
                progress_bar.set_message(format!("Checking free space at {}", path.display()))
            }
            sophon_lib::installer::Update::DownloadingFinished => progress_bar
                .finish_with_message(format!("Finished downloading component {}", matching_field)),
            _ => {}
        }
    }

    pub fn updater_repair<'a>(
        self,
        matching_field: &'a str,
        total_download: u64,
    ) -> impl Fn(sophon_lib::installer::Update) + Clone + 'a {
        let download_style =
                ProgressStyle::default_bar()
                .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("Template should be valid");
        let file_check_style = ProgressStyle::default_bar()
            .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} {percent}%")
            .expect("Template should be valid");

        if let StatusFormatStateInner::ProgressBar(pb) = &self.0 {
            pb.reset();
            pb.set_length(total_download);
            pb.set_style(download_style.clone());
            pb.enable_steady_tick(Duration::from_secs_f32(0.25));
        }

        move |msg| match &self.0 {
            StatusFormatStateInner::ProgressBar(progress_bar) => {
                Self::repair_msg_progerssbar(
                    progress_bar,
                    msg,
                    matching_field,
                    &download_style,
                    &file_check_style,
                );
            }
            StatusFormatStateInner::NdJson => NdJsonMessage::repmsg(msg).print(),
        }
    }

    fn repair_msg_progerssbar(
        progress_bar: &ProgressBar,
        msg: sophon_lib::installer::Update,
        matching_field: &str,
        download_style: &ProgressStyle,
        file_check_style: &ProgressStyle,
    ) {
        match msg {
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
            sophon_lib::installer::Update::DownloadingFinished =>
            {
                #[allow(clippy::collapsible_match, reason = "only in 1.96.0")]
                if !progress_bar.is_finished() {
                    progress_bar.finish_with_message(format!(
                        "Finished repairing component {}",
                        matching_field
                    ))
                }
            }
            _ => {}
        }
    }

    pub fn updater_update<'a>(
        self,
        total_download: u64,
        matching_field: &'a str,
    ) -> impl Fn(sophon_lib::updater::Update) + Clone + 'a {
        let download_style =
                ProgressStyle::default_bar()
                .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("Template should be valid");
        let file_check_style = ProgressStyle::default_bar()
            .template("{msg}\n{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} {percent}%")
            .expect("Template should be valid");

        if let StatusFormatStateInner::ProgressBar(pb) = &self.0 {
            pb.reset();
            pb.set_length(total_download);
            pb.set_style(download_style.clone());
            pb.enable_steady_tick(Duration::from_secs_f32(0.25));
        }

        move |msg| match &self.0 {
            StatusFormatStateInner::ProgressBar(progress_bar) => Self::update_msg_progerssbar(
                progress_bar,
                msg,
                matching_field,
                &download_style,
                &file_check_style,
            ),
            StatusFormatStateInner::NdJson => NdJsonMessage::updmsg(msg).print(),
        }
    }

    fn update_msg_progerssbar(
        progress_bar: &ProgressBar,
        msg: sophon_lib::updater::Update,
        matching_field: &str,
        download_style: &ProgressStyle,
        file_check_style: &ProgressStyle,
    ) {
        match msg {
            sophon_lib::updater::Update::DownloadingProgressBytes {
                downloaded_bytes, ..
            } => {
                progress_bar.set_position(downloaded_bytes);
                #[cfg(feature = "tracy")]
                {
                    let rate = progress_bar.per_sec();
                    tracing_tracy::client::plot!("Downloading speed", rate);
                }
            }
            sophon_lib::updater::Update::CheckingFilesStarted => {
                progress_bar.set_message("Checking existing files");
                progress_bar.set_style(file_check_style.clone());
            }
            sophon_lib::updater::Update::DownloadingStarted(location) => {
                progress_bar.set_message(format!("Updating game at {}", location.display()));
                progress_bar.set_style(download_style.clone());
                progress_bar.set_position(0);
                progress_bar.reset_elapsed();
            }
            sophon_lib::updater::Update::CheckingFreeSpace(path) => {
                progress_bar.set_message(format!("Checking free space at {}", path.display()))
            }
            sophon_lib::updater::Update::DownloadingFinished => progress_bar
                .finish_with_message(format!("Finished updating component `{}`", matching_field)),
            _ => {}
        }
    }
}
