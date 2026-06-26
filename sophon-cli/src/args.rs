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

use clap::{Args, ArgAction, ValueEnum};

use sophon_lib::export::reqwest::{
    ClientBuilder as ReqwestClientBuilder,
    Proxy as ReqwestProxy
};

use sophon_lib::api::SophonApi;
use sophon_lib::patcher::HdiffPatcher;
use sophon_lib::downloader::SophonDownloader;
use sophon_lib::updater::SophonUpdater;

use crate::utils::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum SophonRegion {
    #[value(name = "global")]
    Global,

    #[value(name = "china")]
    China
}

impl From<SophonRegion> for sophon_lib::region::SophonRegion {
    fn from(region: SophonRegion) -> Self {
        match region {
            SophonRegion::Global => Self::Global,
            SophonRegion::China => Self::China
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum VerifyMethod {
    #[value(
        name = "none",
        alias = "disable",
        alias = "disabled"
    )]
    None,

    #[value(
        name = "fast",
        alias = "size",
        alias = "sizes",
        alias = "file-size",
        alias = "files-sizes"
    )]
    Fast,

    #[value(
        name = "full",
        alias = "hash",
        alias = "hashes",
        alias = "file-hash",
        alias = "files-hashes"
    )]
    Full
}

impl std::fmt::Display for VerifyMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Fast => f.write_str("fast"),
            Self::Full => f.write_str("full")
        }
    }
}

impl From<VerifyMethod> for sophon_lib::downloader::SophonDownloaderVerifyMethod {
    fn from(method: VerifyMethod) -> Self {
        match method {
            VerifyMethod::None => Self::None,
            VerifyMethod::Fast => Self::Fast,
            VerifyMethod::Full => Self::Full
        }
    }
}

impl From<VerifyMethod> for sophon_lib::updater::SophonUpdaterVerifyMethod {
    fn from(method: VerifyMethod) -> Self {
        match method {
            VerifyMethod::None => Self::None,
            VerifyMethod::Fast => Self::Fast,
            VerifyMethod::Full => Self::Full
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum OutputFormat {
    #[value(name = "text")]
    Text,

    #[value(name = "json")]
    Json
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Args)]
pub struct SophonApiClientArgs {
    /// API requests user agent string.
    #[arg(long, alias = "api-user-agent")]
    pub user_agent: Option<String>,

    /// API requests proxy.
    #[arg(
        long,
        alias = "proxy-addr",
        alias = "proxy-address",
        alias = "api-proxy",
        alias = "api-proxy-addr",
        alias = "api-proxy-address"
    )]
    pub proxy: Option<String>,

    /// API requests timeout in seconds.
    ///
    /// Supports string values: `1h`, `10m`, `5.5s`.
    #[arg(long, alias = "api-timeout")]
    pub timeout: Option<String>
}

impl SophonApiClientArgs {
    pub fn build(&self) -> anyhow::Result<SophonApi> {
        let mut client = ReqwestClientBuilder::new()
            .user_agent(format!("sophon-tools/v{}", sophon_lib::VERSION));

        if let Some(user_agent) = &self.user_agent {
            client = client.user_agent(user_agent);
        }

        if let Some(proxy) = &self.proxy {
            client = client.proxy(ReqwestProxy::all(proxy)?);
        }

        let mut api_client = SophonApi::from(client.build()?);

        if let Some(timeout) = &self.timeout {
            let timeout = parse_duration_str(timeout)
                .ok_or_else(|| anyhow::anyhow!("invalid timeout value"))?;

            api_client = api_client.with_timeout_all(timeout);
        }

        Ok(api_client)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Args)]
pub struct SophonApiArgs {
    /// Sophon API server region.
    #[arg(
        long, value_enum, default_value_t = SophonRegion::Global,
        alias = "edition",
        alias = "api-region",
        alias = "api-edition"
    )]
    pub region: SophonRegion,

    #[arg(long,
        alias = "launcher",
        alias = "api-launcher-id",
        alias = "api-launcher"
    )]
    pub launcher_id: Option<String>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Args)]
pub struct SophonApiGameComponentArg {
    /// Game component name.
    #[arg(
        long, default_value_t = String::from("game"),
        alias = "component-id",
        alias = "component-name",
        alias = "category",
        alias = "category-id",
        alias = "category-name"
    )]
    pub component: String
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Args)]
pub struct SophonDownloaderArgs {
    /// Verify downloaded manifest.
    #[arg(long, default_value_t = VerifyMethod::Full)]
    pub verify_manifest: VerifyMethod,

    /// Verify downloaded chunks.
    ///
    /// Performing full chunks verification is unnecessary expensive. It's
    /// recommended to keep this option off or to verify chunks sizes only.
    #[arg(long, default_value_t = VerifyMethod::Fast)]
    pub verify_chunks: VerifyMethod,

    /// Verify game files before downloading them.
    ///
    /// If enabled, downloader will verify files stored in the game directory
    /// to exclude already correctly downloaded.
    ///
    /// If disabled, downloader will download and overwrite all the files.
    #[arg(
        long, default_value_t = VerifyMethod::Full,
        alias = "verify-before-download"
    )]
    pub verify_before_downloading: VerifyMethod,

    /// Amount of threads to use in the tokio async runtime.
    ///
    /// Majority of tasks are IO-bound and are executed asynchronously. Setting
    /// this value higher than default value will not improve download speed.
    ///
    /// Download speed is bounded by `target_memory_usage` option.
    #[arg(
        long, short('t'),
        alias = "workers",
        default_value_t = std::thread::available_parallelism()
            .map(|threads| threads.get())
            .unwrap_or(1)
    )]
    pub threads: usize,

    /// Amount of system memory downloader will try to utilize.
    ///
    /// Higher value will allow downloader to download more files in parallel.
    /// The real memory usage will be higher than the user-specified value.
    ///
    /// Supports string values: `0.5gb`, `512mb`, `64kb`.
    #[arg(
        long, short('m'), default_value_t = String::from("256mb"),
        alias = "target-memory",
        alias = "memory-usage",
        alias = "target-memory-size",
        alias = "target-memory-buffer",
        alias = "target-memory-buf",
        alias = "memory-buffer",
        alias = "memory-size",
        alias = "memory-buf",
        alias = "memory",
        alias = "mem"
    )]
    pub target_memory_usage: String,

    /// Amount of attempts downloader will make to download a chunk.
    ///
    /// Sometimes API may reject your chunk download request. This option will
    /// make downloader to silently re-establish connection with the API server
    /// several times before failing to download a chunk.
    #[arg(
        long, short('a'), default_value_t = 3,
        alias = "chunk-downloading-attempts",
        alias = "downloading-attempts",
        alias = "download-attempts",
        alias = "chunk-attempts",
        alias = "attempts"
    )]
    pub chunk_download_attempts: u8,

    /// Timeout of download info manifest fetching.
    ///
    /// If unset, no timeout is used.
    #[arg(
        long,
        alias = "download-manifest-timeout",
        alias = "manifest-timeout"
    )]
    pub fetch_manifest_timeout: Option<String>,

    /// Timeout of 1 MB of chunk downloading.
    ///
    /// If set, downloader will drop chunk download connection if it didn't
    /// finish for `ceil(chunk_size_mb) * timeout`.
    ///
    /// If unset, no chunk download timeout is used.
    #[arg(
        long,
        alias = "fetch-chunk-timeout",
        alias = "fetch-chunks-timeout",
        alias = "download-chunk-timeout",
        alias = "download-chunks-timeout"
    )]
    pub fetch_chunk_per_mb_timeout: Option<String>
}

impl SophonDownloaderArgs {
    pub fn build(&self) -> anyhow::Result<SophonDownloader> {
        let target_memory_usage = parse_memory_str(&self.target_memory_usage)
            .ok_or_else(|| anyhow::anyhow!("invalid target_memory_usage value"))?;

        let mut downloader = SophonDownloader::default()
            .with_verify_manifest(self.verify_manifest.into())
            .with_verify_chunks(self.verify_chunks.into())
            .with_verify_before_downloading(self.verify_before_downloading.into())
            .with_target_memory_usage(target_memory_usage)
            .with_chunk_download_attempts(self.chunk_download_attempts);

        if let Some(timeout) = &self.fetch_manifest_timeout {
            let timeout = parse_duration_str(timeout)
                .ok_or_else(|| anyhow::anyhow!("invalid fetch_manifest_timeout value"))?;

            downloader = downloader.with_fetch_manifest_timeout(timeout);
        }

        if let Some(timeout) = &self.fetch_chunk_per_mb_timeout {
            let timeout = parse_duration_str(timeout)
                .ok_or_else(|| anyhow::anyhow!("invalid fetch_chunk_per_mb_timeout value"))?;

            downloader = downloader.with_fetch_chunk_timeout_per_mb(timeout);
        }

        Ok(downloader)
    }
}

impl From<&SophonUpdaterArgs> for SophonDownloaderArgs {
    fn from(updater_args: &SophonUpdaterArgs) -> Self {
        Self {
            verify_manifest: updater_args.verify_manifest,
            verify_chunks: updater_args.verify_chunks,
            verify_before_downloading: updater_args.verify_before_updating,
            threads: updater_args.threads,
            target_memory_usage: updater_args.target_memory_usage.clone(),
            chunk_download_attempts: updater_args.chunk_download_attempts,
            fetch_manifest_timeout: updater_args.fetch_manifest_timeout.clone(),
            fetch_chunk_per_mb_timeout: updater_args.fetch_chunk_per_mb_timeout.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Args)]
pub struct SophonUpdaterArgs {
    /// Verify downloaded manifest.
    #[arg(long, default_value_t = VerifyMethod::Full)]
    pub verify_manifest: VerifyMethod,

    /// Verify downloaded chunks.
    ///
    /// Performing full chunks verification is unnecessary expensive. It's
    /// recommended to keep this option off or to verify chunks sizes only.
    #[arg(long, default_value_t = VerifyMethod::Fast)]
    pub verify_chunks: VerifyMethod,

    /// Verify game files before updating them.
    ///
    /// If enabled, updater will verify files stored in the game directory
    /// to exclude already correctly updated.
    ///
    /// If disabled, updater will download patches for all the files.
    #[arg(
        long, default_value_t = VerifyMethod::Full,
        alias = "verify-before-update"
    )]
    pub verify_before_updating: VerifyMethod,

    /// Verify game files before patching them.
    ///
    /// If enabled, updater will verify game files before trying to patch them.
    /// On mismatch, file will be silently ignored.
    ///
    /// If disabled, updater will try to patch files even if patches cannot
    /// be applied to them.
    #[arg(
        long, default_value_t = VerifyMethod::Full,
        alias = "verify-before-patch"
    )]
    pub verify_before_patching: VerifyMethod,

    /// Delete unused game files.
    ///
    /// If enabled, updater will delete game files that are marked as unused
    /// in the new game version.
    ///
    /// If disabled, unused game files will be kept in the game directory.
    #[arg(
        long, default_value_t = true,
        value_parser = clap::value_parser!(bool),
        action = ArgAction::Set,
        alias = "delete-unused-files",
        alias = "delete-unused"
    )]
    pub delete_unused_assets: bool,

    /// Patch game files.
    ///
    /// If enabled, updater will try to apply downloaded patches to the game
    /// files.
    ///
    /// If disabled, updater will only download patches without applying them.
    #[arg(
        long, default_value_t = true,
        value_parser = clap::value_parser!(bool),
        action = ArgAction::Set,
        alias = "patch-files",
        alias = "apply-patches",
        alias = "apply-patch",
        alias = "apply-chunks",
        alias = "apply-chunk"
    )]
    pub patch_assets: bool,

    /// Delete chunks after applying them to the game files.
    #[arg(
        long, default_value_t = true,
        value_parser = clap::value_parser!(bool),
        action = ArgAction::Set,
        alias = "delete-applied-patches",
        alias = "delete-applied-patch"
    )]
    pub delete_applied_chunks: bool,

    /// Find invalid or missing assets and repair them.
    ///
    /// This option has effect only if `patch_assets` is enabled.
    #[arg(
        long, default_value_t = VerifyMethod::Fast,
        alias = "repair-broken-files",
        alias = "repair-broken",
        alias = "repair-assets",
        alias = "repair-files"
    )]
    pub repair_broken_assets: VerifyMethod,

    /// Path to the `hpatchz` binary.
    ///
    /// If unset, bundled binary will be extracted to a temporary directory.
    ///
    /// https://github.com/sisong/HDiffPatch
    #[arg(
        long,
        alias = "hpatchz-path",
        alias = "hpatchz",
        alias = "hpatch-binary",
        alias = "hpatch-path",
        alias = "hpatch",
        alias = "patcher-binary",
        alias = "patcher"
    )]
    pub hpatchz_binary: Option<PathBuf>,

    /// Amount of threads to use in the tokio async runtime.
    ///
    /// Majority of tasks are IO-bound and are executed asynchronously. Setting
    /// this value higher than default value will not improve download speed.
    ///
    /// Download speed is bounded by `target_memory_usage` option.
    #[arg(
        long, short('t'),
        alias = "workers",
        default_value_t = std::thread::available_parallelism()
            .map(|threads| threads.get())
            .unwrap_or(1)
    )]
    pub threads: usize,

    /// Amount of system memory updater will try to utilize.
    ///
    /// Higher value will allow updater to download more files in parallel.
    /// The real memory usage will be higher than the user-specified value.
    ///
    /// Supports string values: `0.5gb`, `512mb`, `64kb`.
    #[arg(
        long, short('m'), default_value_t = String::from("256mb"),
        alias = "target-memory",
        alias = "memory-usage",
        alias = "target-memory-size",
        alias = "target-memory-buffer",
        alias = "target-memory-buf",
        alias = "memory-buffer",
        alias = "memory-size",
        alias = "memory-buf",
        alias = "memory",
        alias = "mem"
    )]
    pub target_memory_usage: String,

    /// Amount of attempts updater will make to download a chunk.
    ///
    /// Sometimes API may reject your chunk download request. This option will
    /// make updater to silently re-establish the connection with the API server
    /// several times before failing to download a chunk.
    #[arg(
        long, short('a'), default_value_t = 3,
        alias = "chunk-downloading-attempts",
        alias = "downloading-attempts",
        alias = "download-attempts",
        alias = "chunk-attempts",
        alias = "attempts"
    )]
    pub chunk_download_attempts: u8,

    /// Timeout of download info manifest fetching.
    ///
    /// If unset, no timeout is used.
    #[arg(
        long,
        alias = "download-manifest-timeout",
        alias = "manifest-timeout"
    )]
    pub fetch_manifest_timeout: Option<String>,

    /// Timeout of 1 MB of chunk downloading.
    ///
    /// If set, updater will drop chunk download connection if it didn't
    /// finish for `ceil(chunk_size_mb) * timeout`.
    ///
    /// If unset, no chunk download timeout is used.
    #[arg(
        long,
        alias = "fetch-chunk-timeout",
        alias = "fetch-chunks-timeout",
        alias = "fetch-patch-timeout",
        alias = "fetch-patches-timeout",
        alias = "download-chunk-timeout",
        alias = "download-chunks-timeout",
        alias = "download-patch-timeout",
        alias = "download-patches-timeout"
    )]
    pub fetch_chunk_per_mb_timeout: Option<String>
}

impl SophonUpdaterArgs {
    pub fn build(&self) -> anyhow::Result<SophonUpdater> {
        let target_memory_usage = parse_memory_str(&self.target_memory_usage)
            .ok_or_else(|| anyhow::anyhow!("invalid target_memory_usage value"))?;

        let mut updater = SophonUpdater::default()
            .with_verify_manifest(self.verify_manifest.into())
            .with_verify_chunks(self.verify_chunks.into())
            .with_verify_before_updating(self.verify_before_updating.into())
            .with_verify_before_patching(self.verify_before_patching.into())
            .with_delete_unused_assets(self.delete_unused_assets)
            .with_patch_assets(self.patch_assets)
            .with_delete_applied_chunks(self.delete_applied_chunks)
            .with_target_memory_usage(target_memory_usage)
            .with_chunk_download_attempts(self.chunk_download_attempts);

        if let Some(patcher) = self.hpatchz_binary.clone() {
            updater = updater.with_patcher(HdiffPatcher::from(patcher));
        }

        if let Some(timeout) = &self.fetch_manifest_timeout {
            let timeout = parse_duration_str(timeout)
                .ok_or_else(|| anyhow::anyhow!("invalid fetch_manifest_timeout value"))?;

            updater = updater.with_fetch_manifest_timeout(timeout);
        }

        if let Some(timeout) = &self.fetch_chunk_per_mb_timeout {
            let timeout = parse_duration_str(timeout)
                .ok_or_else(|| anyhow::anyhow!("invalid fetch_chunk_per_mb_timeout value"))?;

            updater = updater.with_fetch_chunk_timeout_per_mb(timeout);
        }

        Ok(updater)
    }
}
