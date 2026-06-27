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

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use regex::Regex;

use sophon_lib::downloader::SophonDownloader;
use sophon_lib::verifier::{SophonVerifier, VerifyResult};

use crate::args::*;
use crate::commands::*;

fn is_regex_match(regex: Option<&Regex>, path: &str) -> bool {
    regex.map(|regex| regex.is_match(path)).unwrap_or(true)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    game_dir: PathBuf,
    api_args: SophonApiArgs,
    component: SophonApiGameComponentArg,
    version: Option<String>,
    regex: Option<String>,
    fast_verify: bool,
    show_missing: bool,
    threads: usize,
    api_client: SophonApiClientArgs,
    output_format: OutputFormat
) -> anyhow::Result<()> {
    let regex = regex.as_deref()
        .map(Regex::new)
        .transpose()?;

    if !game_dir.is_dir() {
        return Ok(());
    }

    let mut entries = VecDeque::from([game_dir.clone()]);

    while let Some(path) = entries.pop_back() {
        if !path.is_dir() {
            entries.push_front(path);

            break;
        }

        for entry in path.read_dir()? {
            let path = entry?.path();

            if path.is_dir() {
                entries.push_back(path);
            } else {
                entries.push_front(path);
            }
        }
    }

    entries.retain(|path| {
        is_regex_match(regex.as_ref(), &path.to_string_lossy())
    });

    if entries.is_empty() {
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;

    let api = api_client.build()?;

    let game = api.game(
        api_args.region.into(),
        api_args.launcher_id,
        game_id
    );

    let package = runtime.block_on(game.package(version))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let Some(download_manifest) = runtime.block_on(package.find_download_manifest(&component.component))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
    else {
        return Ok(());
    };

    let download_info = runtime.block_on(
        SophonDownloader::default()
            .with_client(api.into())
            .with_runtime(runtime.handle().clone())
            .fetch_download_info(&download_manifest)
    )?;

    let download_info = Arc::unwrap_or_clone(download_info);

    let scanned_paths = Arc::new(Mutex::new(HashSet::with_capacity(entries.len())));

    let mut verifier = SophonVerifier::from(download_info.assets)
        .with_runtime(runtime.handle().clone())
        .with_fast_verify(fast_verify);

    match output_format {
        OutputFormat::Text => {
            let view = nutmeg::View::new(
                ProgressBar {
                    current: 0,
                    total: 0,
                    prefix: String::from("Verify"),
                    format_bytes: true
                },
                nutmeg::Options::new()
                    .print_holdoff(std::time::Duration::ZERO)
            );

            // Scan installed game files.
            {
                let scanned_paths = scanned_paths.clone();

                runtime.block_on(verifier.scan_directory(
                    game_dir.clone(),
                    Arc::new(move |update| {
                        if let Ok(mut lock) = scanned_paths.lock() {
                            lock.insert(update.path.clone());
                        }

                        match update.result {
                            VerifyResult::Valid   => view.message(format!("      valid  {:#?}\n", update.path)),
                            VerifyResult::Invalid => view.message(format!("[!] invalid  {:#?}\n", update.path)),
                            VerifyResult::Unknown => view.message(format!("[?] unknown  {:#?}\n", update.path)),
                        };

                        view.update(move |model| {
                            model.current = update.current;
                            model.total = update.total;
                        });
                    })
                ))?;
            }

            // Show missing files.
            if let Ok(lock) = scanned_paths.lock() && show_missing {
                for path in verifier.assets()
                    .iter()
                    .map(|asset| game_dir.join(&asset.path))
                    .filter(|path| !lock.contains(path))
                    .filter(|path| is_regex_match(regex.as_ref(), &path.to_string_lossy()))
                {
                    println!("[!] missing  {path:#?}");
                }
            }
        }

        OutputFormat::Json => {
            // Scan installed game files.
            {
                let scanned_paths = scanned_paths.clone();

                runtime.block_on(verifier.scan_directory(
                    game_dir.clone(),
                    Arc::new(move |update| {
                        if let Ok(mut lock) = scanned_paths.lock() {
                            lock.insert(update.path.clone());
                        }

                        let result = serde_json::json!({
                            "available": {
                                "current": update.current,
                                "total": update.total,
                                "path": update.path,
                                "result": match update.result {
                                    VerifyResult::Valid => "valid",
                                    VerifyResult::Invalid => "invalid",
                                    VerifyResult::Unknown => "unknown"
                                }
                            }
                        });

                        if let Ok(result) = serde_json::to_string(&result) {
                            println!("{result}");
                        }
                    })
                ))?;
            }

            // Show missing files.
            if let Ok(lock) = scanned_paths.lock() && show_missing {
                for path in verifier.assets()
                    .iter()
                    .map(|asset| game_dir.join(&asset.path))
                    .filter(|path| !lock.contains(path))
                    .filter(|path| is_regex_match(regex.as_ref(), &path.to_string_lossy()))
                {
                    println!("{}", serde_json::to_string(&serde_json::json!({
                        "missing": {
                            "path": path,
                            "result": "missing"
                        }
                    }))?);
                }
            }
        }
    }

    Ok(())
}
