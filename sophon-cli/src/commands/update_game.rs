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

use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use regex::Regex;

use sophon_lib::verifier::VerifyResult;
use sophon_lib::downloader::SophonDownloaderProgressMsg;
use sophon_lib::updater::SophonUpdaterProgressMsg;

use crate::args::*;
use crate::commands::*;

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    game_dir: PathBuf,
    api_args: SophonApiArgs,
    component: SophonApiGameComponentArg,
    from_version: Option<String>,
    to_version: Option<String>,
    chunks_dir: PathBuf,
    regex: Option<String>,
    api_client: SophonApiClientArgs,
    updater_args: SophonUpdaterArgs,
    output_format: OutputFormat
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(updater_args.threads.max(1))
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

    // Detect current game version.
    let from_version = match from_version {
        Some(version) => version,
        None => {
            runtime.block_on(game.detect_version(&game_dir))
                .map_err(|err| anyhow::anyhow!(err.to_string()))?
                .ok_or_else(|| anyhow::anyhow!("failed to detect installed game version"))?
        }
    };

    // Fetch game update manifest.
    let package = runtime.block_on(game.package(to_version))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let Some(update_manifest) = runtime.block_on(package.find_update_manifest(&component.component))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
    else {
        return Ok(());
    };

    // Prepare game updater.
    let mut updater = updater_args.build()?
        .with_client(api.client().clone())
        .with_runtime(runtime.handle().clone());

    if let Some(regex) = regex.clone() {
        updater = updater.with_assets_filter(Box::new(move |asset| {
            regex.is_match(&asset.path)
        }));
    }

    let view = Arc::new(Mutex::new({
        match output_format {
            OutputFormat::Text => {
                Some(nutmeg::View::new(
                    ProgressBar {
                        current: 0,
                        total: 0,
                        prefix: String::new(),
                        format_bytes: true
                    },
                    nutmeg::Options::default()
                ))
            }

            OutputFormat::Json => None
        }
    }));

    // Update game files.
    {
        let view = view.clone();

        runtime.block_on(updater.update(
            &update_manifest,
            &from_version,
            &chunks_dir,
            &game_dir,
            Arc::new(move |update| {
                match output_format {
                    OutputFormat::Text => {
                        match update {
                            SophonUpdaterProgressMsg::Verify {
                                current,
                                total,
                                ..
                            } => {
                                if let Ok(Some(view)) = view.lock().as_deref() {
                                    view.update(|model| {
                                        model.current = current;
                                        model.total = total;
                                        model.prefix = String::from("Verify");
                                    });
                                }
                            }

                            SophonUpdaterProgressMsg::Download {
                                current,
                                total
                            } => {
                                if let Ok(Some(view)) = view.lock().as_deref() {
                                    view.update(|model| {
                                        model.current = current;
                                        model.total = total;
                                        model.prefix = String::from("Download");
                                    });
                                }
                            }

                            SophonUpdaterProgressMsg::Patch {
                                current,
                                total,
                                ..
                            } => {
                                if let Ok(Some(view)) = view.lock().as_deref() {
                                    view.update(|model| {
                                        model.current = current;
                                        model.total = total;
                                        model.prefix = String::from("Patch");
                                    });
                                }
                            }
                        }
                    }

                    OutputFormat::Json => {
                        match update {
                            SophonUpdaterProgressMsg::Verify {
                                current,
                                total,
                                path,
                                result
                            } => {
                                let result = serde_json::json!({
                                    "updater": {
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
                                    }
                                });

                                if let Ok(result) = serde_json::to_string(&result) {
                                    println!("{result}");
                                }
                            }

                            SophonUpdaterProgressMsg::Download {
                                current,
                                total
                            } => {
                                let result = serde_json::json!({
                                    "updater": {
                                        "download": {
                                            "current": current,
                                            "total": total
                                        }
                                    }
                                });

                                if let Ok(result) = serde_json::to_string(&result) {
                                    println!("{result}");
                                }
                            }

                            SophonUpdaterProgressMsg::Patch {
                                current,
                                total,
                                path,
                                result
                            } => {
                                let result = serde_json::json!({
                                    "updater": {
                                        "patch": {
                                            "current": current,
                                            "total": total,
                                            "path": path,
                                            "result": result
                                        }
                                    }
                                });

                                if let Ok(result) = serde_json::to_string(&result) {
                                    println!("{result}");
                                }
                            }
                        }
                    }
                }
            })
        ))?;
    }

    // If updater has applied some patches and the user has enabled game assets
    // repairing.
    if updater_args.patch_assets
        && updater_args.repair_broken_assets != VerifyMethod::None
    {
        let Some(download_manifest) = runtime.block_on(package.find_download_manifest(&component.component))
            .map_err(|err| anyhow::anyhow!(err.to_string()))?
        else {
            return Ok(());
        };

        // Prepare repairer.
        let mut downloader = SophonDownloaderArgs::from(&updater_args).build()?
            .with_client(api.into())
            .with_runtime(runtime.handle().clone())
            .with_verify_before_downloading(updater_args.repair_broken_assets.into());

        if let Some(regex) = regex {
            downloader = downloader.with_assets_filter(Box::new(move |asset| {
                regex.is_match(&asset.path)
            }));
        }

        // Repair game files.
        runtime.block_on(downloader.download(
            &download_manifest,
            &game_dir,
            Arc::new(move |update| {
                match output_format {
                    OutputFormat::Text => {
                        match update {
                            SophonDownloaderProgressMsg::Verify {
                                current,
                                total,
                                ..
                            } => {
                                if let Ok(Some(view)) = view.lock().as_deref() {
                                    view.update(|model| {
                                        model.current = current;
                                        model.total = total;
                                        model.prefix = String::from("Validate");
                                    });
                                }
                            }

                            SophonDownloaderProgressMsg::Download {
                                current,
                                total
                            } => {
                                if let Ok(Some(view)) = view.lock().as_deref() {
                                    view.update(|model| {
                                        model.current = current;
                                        model.total = total;
                                        model.prefix = String::from("Repair");
                                    });
                                }
                            }
                        }
                    }

                    OutputFormat::Json => {
                        match update {
                            SophonDownloaderProgressMsg::Verify {
                                current,
                                total,
                                path,
                                result
                            } => {
                                let result = serde_json::json!({
                                    "repairer": {
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
                                    }
                                });

                                if let Ok(result) = serde_json::to_string(&result) {
                                    println!("{result}");
                                }
                            }

                            SophonDownloaderProgressMsg::Download {
                                current,
                                total
                            } => {
                                let result = serde_json::json!({
                                    "repairer": {
                                        "download": {
                                            "current": current,
                                            "total": total
                                        }
                                    }
                                });

                                if let Ok(result) = serde_json::to_string(&result) {
                                    println!("{result}");
                                }
                            }
                        }
                    }
                }
            })
        ))?;
    }

    Ok(())
}
