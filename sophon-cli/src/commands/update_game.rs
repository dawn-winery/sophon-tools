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

use std::path::PathBuf;

use regex::Regex;

use crate::args::*;

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
    _output_format: OutputFormat
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

    runtime.block_on(updater.update(
        &update_manifest,
        &from_version,
        &chunks_dir,
        &game_dir
    ))?;

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

        let mut downloader = SophonDownloaderArgs::from(&updater_args).build()?
            .with_client(api.into())
            .with_runtime(runtime.handle().clone())
            .with_verify_before_downloading(updater_args.repair_broken_assets.into());

        if let Some(regex) = regex {
            downloader = downloader.with_assets_filter(Box::new(move |asset| {
                regex.is_match(&asset.path)
            }));
        }

        runtime.block_on(downloader.download(
            &download_manifest,
            &game_dir,
            Box::new(|_, _| {})
        ))?;
    }

    Ok(())
}
