// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@vk.com>
//                     "John the Cooling Fan"
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::sync::Arc;
use std::path::PathBuf;

use regex::Regex;

use sophon_lib::verifier::VerifyResult;
use sophon_lib::downloader::SophonDownloaderProgressMsg;

use crate::args::*;
use crate::commands::*;

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    game_dir: PathBuf,
    api_args: SophonApiArgs,
    component: SophonApiGameComponentArg,
    version: Option<String>,
    regex: Option<String>,
    api_client: SophonApiClientArgs,
    downloader: SophonDownloaderArgs,
    output_format: OutputFormat
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(downloader.threads.max(1))
        .enable_all()
        .build()?;

    let regex = regex.as_deref()
        .map(Regex::new)
        .transpose()?;

    let api = api_client.build()?;

    let game = api.game(
        api_args.region.into(),
        api_args.launcher_id,
        game_id
    );

    // Fetch game download manifest.
    let package = runtime.block_on(game.package(version))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let Some(download_manifest) = runtime.block_on(package.find_download_manifest(&component.component))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
    else {
        return Ok(());
    };

    // Prepare downloader.
    let mut downloader = downloader.build()?
        .with_client(api.into())
        .with_runtime(runtime.handle().clone());

    if let Some(regex) = regex {
        downloader = downloader.with_assets_filter(Box::new(move |asset| {
            regex.is_match(&asset.path)
        }));
    }

    match output_format {
        OutputFormat::Text => {
            let view = nutmeg::View::new(
                ProgressBar {
                    current: 0,
                    total: 0,
                    prefix: String::new(),
                    format_bytes: true
                },
                nutmeg::Options::default()
            );

            runtime.block_on(downloader.download(
                &download_manifest,
                &game_dir,
                Arc::new(move |update| {
                    view.update(move |model| {
                        match update {
                            SophonDownloaderProgressMsg::Verify {
                                current,
                                total,
                                ..
                            } => {
                                model.current = current;
                                model.total = total;
                                model.prefix = String::from("Verify");
                            }

                            SophonDownloaderProgressMsg::Download {
                                current,
                                total
                            } => {
                                model.current = current;
                                model.total = total;
                                model.prefix = String::from("Download");
                            }
                        }
                    })
                })
            ))?;
        }

        OutputFormat::Json => {
            runtime.block_on(downloader.download(
                &download_manifest,
                &game_dir,
                Arc::new(|update| {
                    let msg = match update {
                        SophonDownloaderProgressMsg::Verify {
                            current,
                            total,
                            path,
                            result
                        } => {
                            serde_json::to_string(&serde_json::json!({
                                "verify": {
                                    "current": current,
                                    "total": total,
                                    "path": path,
                                    "result": match result {
                                        VerifyResult::Valid => "valid",
                                        VerifyResult::Invalid => "invalid",
                                        VerifyResult::Unknown => "unknown"
                                    }
                                }
                            }))
                        }

                        SophonDownloaderProgressMsg::Download { current, total } => {
                            serde_json::to_string(&serde_json::json!({
                                "download": {
                                    "current": current,
                                    "total": total
                                }
                            }))
                        }
                    };

                    if let Ok(msg) = msg {
                        println!("{msg}");
                    }
                })
            ))?;
        }
    }

    Ok(())
}
