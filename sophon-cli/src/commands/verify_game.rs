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

use std::collections::VecDeque;
use std::path::PathBuf;
use std::io::Write;

use regex::Regex;

use sophon_lib::api::SophonApi;
use sophon_lib::downloader::SophonDownloader;
use sophon_lib::verifier::{SophonVerifier, VerifyResult};

use super::{SophonRegion, OutputFormat, ProgressBar, reqwest_client};

fn is_regex_match(regex: Option<&Regex>, path: &str) -> bool {
    regex.map(|regex| regex.is_match(path)).unwrap_or(true)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    component_id: String,
    version: Option<String>,
    path: PathBuf,
    region: SophonRegion,
    launcher_id: Option<String>,
    regex: Option<String>,
    fast_verify: bool,
    user_agent: Option<String>,
    proxy: Option<String>,
    output_format: OutputFormat
) -> anyhow::Result<()> {
    let regex = regex.as_deref()
        .map(Regex::new)
        .transpose()?;

    if !path.is_dir() {
        return Ok(());
    }

    let mut entries = VecDeque::from([path]);

    while let Some(path) = entries.pop_back() {
        if !path.is_dir() {
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

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let api = SophonApi::from(reqwest_client(user_agent, proxy)?.build()?);

    let game = api.game(region.into(), launcher_id, game_id);

    let package = runtime.block_on(game.package(version))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let Some(download_manifest) = runtime.block_on(package.find_download_manifest(&component_id))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
    else {
        return Ok(());
    };

    let download_info = runtime.block_on(SophonDownloader::default()
        .fetch_download_info(&download_manifest))?;

    let mut verifier = SophonVerifier::from(download_info.assets)
        .with_fast_verify(fast_verify);

    match output_format {
        OutputFormat::Text => {
            let total = entries.len();

            let mut view = nutmeg::View::new(
                ProgressBar {
                    current: 0,
                    total: total as u64,
                    format_bytes: false
                },
                nutmeg::Options::new()
                    .print_holdoff(std::time::Duration::ZERO)
            );

            for path in entries.into_iter() {
                match verifier.verify_file(path.clone())? {
                    VerifyResult::Valid   => writeln!(view, "      valid  {path:#?}")?,
                    VerifyResult::Invalid => writeln!(view, "[!] invalid  {path:#?}")?,
                    VerifyResult::Unknown => writeln!(view, "[?] unknown  {path:#?}")?
                }

                view.update(|model| model.current += 1);
            }
        }

        OutputFormat::Json => {
            let total = entries.len();

            for (i, path) in entries.into_iter().enumerate() {
                let result = &serde_json::json!({
                    "path": path,
                    "current": i + 1,
                    "total": total,
                    "result": match verifier.verify_file(path)? {
                        VerifyResult::Valid => "valid",
                        VerifyResult::Invalid => "invalid",
                        VerifyResult::Unknown => "unknown"
                    }
                });

                println!("{}", serde_json::to_string(result)?);
            }
        }
    }

    Ok(())
}
