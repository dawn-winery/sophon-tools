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

use sophon_lib::export::reqwest::{
    Client as ReqwestClient,
    Proxy as ReqwestProxy
};
use sophon_lib::api::SophonApi;
use sophon_lib::downloader::{SophonDownloader, SophonDownloaderVerifyMethod};

use super::{SophonRegion, OutputFormat};

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    component_id: String,
    version: Option<String>,
    path: PathBuf,
    region: SophonRegion,
    launcher_id: Option<String>,
    regex: Option<String>,
    threads: usize,
    target_memory_usage: u64,
    fast_verify: bool,
    no_verify_before_download: bool,
    user_agent: Option<String>,
    proxy: Option<String>,
    output_format: OutputFormat,
    _ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .enable_all()
        .build()?;

    let regex = regex.as_deref()
        .map(Regex::new)
        .transpose()?;

    let api = SophonApi::default();

    let game = api.game(region.into(), launcher_id, game_id);

    let package = runtime.block_on(game.package(version))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let Some(download_manifest) = runtime.block_on(package.find_download_manifest(&component_id))
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
    else {
        return Ok(());
    };

    let mut downloader = SophonDownloader::default()
        .with_runtime(runtime.handle().clone())
        .with_target_memory_usage(target_memory_usage)
        .with_verify_before_downloading({
            if no_verify_before_download {
                SophonDownloaderVerifyMethod::None
            } else if fast_verify {
                SophonDownloaderVerifyMethod::Fast
            } else {
                SophonDownloaderVerifyMethod::Full
            }
        });

    match (user_agent, proxy) {
        (Some(user_agent), Some(proxy)) => {
            downloader = downloader.with_client(
                ReqwestClient::builder()
                    .user_agent(user_agent)
                    .proxy(ReqwestProxy::all(proxy)?)
                    .build()?
            );
        }

        (Some(user_agent), None) => {
            downloader = downloader.with_client(
                ReqwestClient::builder()
                    .user_agent(user_agent)
                    .build()?
            );
        }

        (None, Some(proxy)) => {
            downloader = downloader.with_client(
                ReqwestClient::builder()
                    .user_agent(format!("sophon-tools/v{}", sophon_lib::VERSION))
                    .proxy(ReqwestProxy::all(proxy)?)
                    .build()?
            );
        }

        (None, None) => ()
    }

    if let Some(regex) = regex {
        downloader = downloader.with_assets_filter(Box::new(move |asset| {
            regex.is_match(&asset.path)
        }));
    }

    match output_format {
        OutputFormat::Text => {
            runtime.block_on(downloader.download(
                &download_manifest,
                &path,
                Box::new(|current, total| {
                    println!(
                        "{:.2} MB / {:.2} MB",
                        current as f64 / 1024.0 / 1024.0,
                        total as f64 / 1024.0 / 1024.0
                    );
                })
            ))?;
        }

        OutputFormat::Json => {
            runtime.block_on(downloader.download(
                &download_manifest,
                &path,
                Box::new(|current, total| {
                    if let Ok(msg) = serde_json::to_string(&serde_json::json!({
                        "current": current,
                        "total": total
                    })) {
                        println!("{msg}");
                    }
                })
            ))?;
        }
    }

    Ok(())
}
