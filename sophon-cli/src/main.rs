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

use tracing_subscriber::prelude::*;

use clap::{Parser, Subcommand, ArgAction};

pub mod commands;

use commands::*;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Enable trace logs in stderr.
    #[arg(long, alias = "log", alias = "trace")]
    logs: bool,

    /// Disable unicode output.
    #[arg(long)]
    ascii: bool,

    #[command(subcommand)]
    command: CliCommands
}

#[derive(Debug, Subcommand)]
enum CliCommands {
    /// Perform Sophon API requests.
    #[command(subcommand)]
    Api(CliApiCommands),

    /// Detect installed game.
    Detect {
        #[arg(index = 1, required = true)]
        path: PathBuf,

        /// API requests user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API requests proxy.
        #[arg(long)]
        proxy: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Verify game.
    Verify {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(index = 2, required = true)]
        path: PathBuf,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        #[arg(long)]
        version: Option<String>,

        #[arg(
            long, default_value_t = String::from("game"),
            alias = "component-id",
            alias = "component-name",
            alias = "category",
            alias = "category-id",
            alias = "category-name"
        )]
        component: String,

        /// Verify files that match the regex.
        #[arg(long)]
        regex: Option<String>,

        /// Use files sizes for verification instead of calculating md5 hashes.
        #[arg(long, alias = "fast", alias = "fast-verifying")]
        fast_verify: bool,

        /// API requests user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API requests proxy.
        #[arg(long)]
        proxy: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Download game.
    Download {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(index = 2, required = true)]
        path: PathBuf,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        #[arg(long)]
        version: Option<String>,

        #[arg(
            long, default_value_t = String::from("game"),
            alias = "component-id",
            alias = "component-name",
            alias = "category",
            alias = "category-id",
            alias = "category-name"
        )]
        component: String,

        /// Download files that match the regex.
        #[arg(long)]
        regex: Option<String>,

        /// Amount of threads to use in the tokio async runtime.
        #[arg(
            long, short('t'),
            alias = "workers",
            default_value_t = std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1)
        )]
        threads: usize,

        /// Amount of system memory downloader will try to utilize.
        ///
        /// Higher value will allow downloader to download more files in
        /// parallel.
        #[arg(
            long, short('m'), default_value_t = String::from("256mb"),
            alias = "target-memory",
            alias = "memory-usage",
            alias = "target-memory-buffer",
            alias = "target-memory-buf",
            alias = "memory-buffer",
            alias = "memory-buf",
            alias = "memory",
            alias = "mem"
        )]
        target_memory_usage: String,

        /// Verify downloaded manifest.
        #[arg(long, default_value_t = VerifyMethod::Full)]
        verify_manifest: VerifyMethod,

        /// Verify downloaded chunks.
        #[arg(long, default_value_t = VerifyMethod::Fast)]
        verify_chunks: VerifyMethod,

        /// Verify game files before downloading them.
        ///
        /// If disabled, downloader will overwrite game files even if they're
        /// already properly downloaded.
        #[arg(
            long, default_value_t = VerifyMethod::Full,
            alias = "verify-before-download"
        )]
        verify_before_downloading: VerifyMethod,

        /// Downloader user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// Downloader proxy.
        #[arg(long)]
        proxy: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Update game.
    Update {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(index = 2, required = true)]
        path: PathBuf,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        /// Currently installed version of the game.
        ///
        /// If unset, it will be guessed from the installed game files.
        #[arg(long)]
        from_version: Option<String>,

        /// Version of the game to which it should be updated.
        ///
        /// If unset, the latest available game version will be used.
        #[arg(long)]
        to_version: Option<String>,

        #[arg(
            long, default_value = ".cache",
            alias = "chunks-path",
            alias = "download-dir",
            alias = "download-path",
            alias = "chunks-download-dir",
            alias = "chunks-download-path"
        )]
        chunks_dir: PathBuf,

        #[arg(
            long, default_value_t = String::from("game"),
            alias = "component-id",
            alias = "component-name",
            alias = "category",
            alias = "category-id",
            alias = "category-name"
        )]
        component: String,

        /// Update files that match the regex.
        #[arg(long)]
        regex: Option<String>,

        /// Amount of threads to use in the tokio async runtime.
        #[arg(
            long, short('t'),
            alias = "workers",
            default_value_t = std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1)
        )]
        threads: usize,

        /// Amount of system memory updater will try to utilize.
        ///
        /// Higher value will allow updater to download more files in parallel.
        #[arg(
            long, short('m'), default_value_t = String::from("256mb"),
            alias = "target-memory",
            alias = "memory-usage",
            alias = "target-memory-buffer",
            alias = "target-memory-buf",
            alias = "memory-buffer",
            alias = "memory-buf",
            alias = "memory",
            alias = "mem"
        )]
        target_memory_usage: String,

        /// Verify downloaded manifest.
        #[arg(long, default_value_t = VerifyMethod::Full)]
        verify_manifest: VerifyMethod,

        /// Verify downloaded chunks.
        #[arg(long, default_value_t = VerifyMethod::Fast)]
        verify_chunks: VerifyMethod,

        /// Verify game files before updating them.
        ///
        /// If disabled, updater will try to update game files even if they're
        /// already updated.
        #[arg(
            long, default_value_t = VerifyMethod::Full,
            alias = "verify-before-update"
        )]
        verify_before_updating: VerifyMethod,

        /// Delete unused game files.
        #[arg(
            long, default_value_t = true,
            value_parser = clap::value_parser!(bool),
            action = ArgAction::Set,
            alias = "delete-unused"
        )]
        delete_unused_files: bool,

        /// Patch game files.
        #[arg(
            long, default_value_t = true,
            value_parser = clap::value_parser!(bool),
            action = ArgAction::Set,
            alias = "apply-patches",
            alias = "apply-chunks"
        )]
        patch_files: bool,

        /// Delete chunks after applying them to the game files.
        #[arg(
            long, default_value_t = true,
            value_parser = clap::value_parser!(bool),
            action = ArgAction::Set
        )]
        delete_chunks: bool,

        /// Downloader user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// Downloader proxy.
        #[arg(long)]
        proxy: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    }
}

#[derive(Debug, Subcommand)]
enum CliApiCommands {
    /// List information about available games.
    #[command(alias = "games")]
    ListGames {
        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        /// API request user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API request proxy.
        #[arg(long)]
        proxy: Option<String>,

        /// API request timeout in seconds.
        #[arg(long)]
        timeout: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// List information about game components.
    #[command(
        alias = "list-categories",
        alias = "list-game-components",
        alias = "list-game-categories",
        alias = "game-components",
        alias = "game-categories",
        alias = "components",
        alias = "categories"
    )]
    ListComponents {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        /// Show all non-standard components.
        #[arg(long)]
        show_all: bool,

        /// API request user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API request proxy.
        #[arg(long)]
        proxy: Option<String>,

        /// API request timeout in seconds.
        #[arg(long)]
        timeout: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Get list of all the game versions.
    #[command(
        alias = "versions",
        alias = "list-game-versions"
    )]
    GameVersions {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        /// API request user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API request proxy.
        #[arg(long)]
        proxy: Option<String>,

        /// API request timeout in seconds.
        #[arg(long)]
        timeout: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Get download information of the given game version and component.
    #[command(
        alias = "game-info",
        alias = "package-info",
        alias = "package-download-info",
        alias = "download-info"
    )]
    GameDownloadInfo {
        #[arg(index = 1, required = true)]
        game: String,

        #[arg(index = 2, default_value_t = String::from("game"))]
        component: String,

        #[arg(index = 3)]
        version: Option<String>,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

        /// Show files that match the regex.
        #[arg(long)]
        regex: Option<String>,

        /// API request user agent string.
        #[arg(long)]
        user_agent: Option<String>,

        /// API request proxy.
        #[arg(long)]
        proxy: Option<String>,

        /// API request timeout in seconds.
        #[arg(long)]
        timeout: Option<String>,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.logs {
        let logger = tracing_subscriber::fmt::layer()
            .with_ansi(!cli.ascii)
            .with_writer(std::io::stderr)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                !metadata.target().contains("rustls")
                    && !metadata.target().contains("reqwest")
                    && !metadata.target().contains("h2")
                    && !metadata.target().contains("hyper_util")
            }));

        tracing_subscriber::registry()
            .with(logger)
            .init();
    }

    match cli.command {
        CliCommands::Api(CliApiCommands::ListGames {
            region,
            launcher_id,
            user_agent,
            proxy,
            timeout,
            output_format
        }) => {
            let timeout = timeout.as_deref()
                .map(|timeout| {
                    parse_time_str(timeout)
                        .ok_or_else(|| anyhow::anyhow!("invalid timeout value"))
                })
                .transpose()?;

            list_games::run(
                region,
                launcher_id,
                user_agent,
                proxy,
                timeout,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::ListComponents {
            game,
            region,
            launcher_id,
            show_all,
            user_agent,
            proxy,
            timeout,
            output_format
        }) => {
            let timeout = timeout.as_deref()
                .map(|timeout| {
                    parse_time_str(timeout)
                        .ok_or_else(|| anyhow::anyhow!("invalid timeout value"))
                })
                .transpose()?;

            list_components::run(
                game,
                region,
                launcher_id,
                show_all,
                user_agent,
                proxy,
                timeout,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::GameVersions {
            game,
            region,
            launcher_id,
            user_agent,
            proxy,
            timeout,
            output_format
        }) => {
            let timeout = timeout.as_deref()
                .map(|timeout| {
                    parse_time_str(timeout)
                        .ok_or_else(|| anyhow::anyhow!("invalid timeout value"))
                })
                .transpose()?;

            game_versions::run(
                game,
                region,
                launcher_id,
                user_agent,
                proxy,
                timeout,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::GameDownloadInfo {
            game,
            component,
            version,
            region,
            launcher_id,
            regex,
            user_agent,
            proxy,
            timeout,
            output_format
        }) => {
            let timeout = timeout.as_deref()
                .map(|timeout| {
                    parse_time_str(timeout)
                        .ok_or_else(|| anyhow::anyhow!("invalid timeout value"))
                })
                .transpose()?;

            download_info::run(
                game,
                component,
                version,
                region,
                launcher_id,
                regex,
                user_agent,
                proxy,
                timeout,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Detect {
            path,
            user_agent,
            proxy,
            output_format
        } => detect_game::run(
            path,
            user_agent,
            proxy,
            output_format,
            cli.ascii
        ),

        CliCommands::Verify {
            game,
            path,
            region,
            launcher_id,
            version,
            component,
            regex,
            fast_verify,
            user_agent,
            proxy,
            output_format
        } => verify_game::run(
            game,
            component,
            version,
            path,
            region,
            launcher_id,
            regex,
            fast_verify,
            user_agent,
            proxy,
            output_format
        ),

        CliCommands::Download {
            game,
            path,
            region,
            launcher_id,
            version,
            component,
            regex,
            threads,
            target_memory_usage,
            verify_manifest,
            verify_chunks,
            verify_before_downloading,
            user_agent,
            proxy,
            output_format
        } => {
            let target_memory_usage = parse_memory_str(&target_memory_usage)
                .ok_or_else(|| anyhow::anyhow!("invalid target_memory_usage value"))?;

            download_game::run(
                game,
                component,
                version,
                path,
                region,
                launcher_id,
                regex,
                threads,
                target_memory_usage,
                verify_manifest,
                verify_chunks,
                verify_before_downloading,
                user_agent,
                proxy,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Update {
            game,
            path,
            region,
            launcher_id,
            from_version,
            to_version,
            chunks_dir,
            component,
            regex,
            threads,
            target_memory_usage,
            verify_manifest,
            verify_chunks,
            verify_before_updating,
            delete_unused_files,
            patch_files,
            delete_chunks,
            user_agent,
            proxy,
            output_format
        } => {
            let target_memory_usage = parse_memory_str(&target_memory_usage)
                .ok_or_else(|| anyhow::anyhow!("invalid target_memory_usage value"))?;

            update_game::run(
                game,
                component,
                from_version,
                to_version,
                chunks_dir,
                path,
                region,
                launcher_id,
                regex,
                threads,
                target_memory_usage,
                verify_manifest,
                verify_chunks,
                verify_before_updating,
                delete_unused_files,
                patch_files,
                delete_chunks,
                user_agent,
                proxy,
                output_format,
                cli.ascii
            )
        }
    }
}
