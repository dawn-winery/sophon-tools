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

use clap::{Parser, Subcommand};

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

        /// Amount of threads to use in the tokio async runtime. If unset,
        /// amount of virtual CPU cores will be used.
        #[arg(
            long, short('t'),
            alias = "workers",
            default_value_t = std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1)
        )]
        threads: usize,

        /// Amount of system memory downloader will try to utilize. Higher value
        /// will allow downloader to download more files in parallel.
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

        /// Use files sizes for verification instead of calculating md5 hashes.
        #[arg(long, alias = "fast", alias = "fast-verifying")]
        fast_verify: bool,

        /// Do not verify files before downloading.
        #[arg(
            long,
            alias = "no-verify-before-downloading",
            alias = "no-verifying-before-download",
            alias = "no-verifying-before-downloading",
            alias = "skip-verify-before-download",
            alias = "skip-verify-before-downloading",
            alias = "skip-verifying-before-download",
            alias = "skip-verifying-before-downloading"
        )]
        no_verify_before_download: bool,

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

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat,

        #[arg(long)]
        show_all: bool
    },

    /// Get list of all the game versions.
    #[command(alias = "versions")]
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
            output_format
        }) => list_games::run(region, launcher_id, output_format, cli.ascii),

        CliCommands::Api(CliApiCommands::ListComponents {
            game,
            region,
            launcher_id,
            output_format,
            show_all
        }) => list_components::run(
            game,
            region,
            launcher_id,
            output_format,
            show_all,
            cli.ascii
        ),

        CliCommands::Api(CliApiCommands::GameVersions {
            game,
            region,
            launcher_id,
            output_format
        }) => game_versions::run(
            game,
            region,
            launcher_id,
            output_format,
            cli.ascii
        ),

        CliCommands::Api(CliApiCommands::GameDownloadInfo {
            game,
            component,
            version,
            region,
            launcher_id,
            regex,
            output_format
        }) => download_info::run(
            game,
            component,
            version,
            region,
            launcher_id,
            regex,
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
            target_memory_usage: target_memory_usage_str,
            fast_verify,
            no_verify_before_download,
            user_agent,
            proxy,
            output_format
        } => {
            const MULTIPLIERS: &[(&str, f64)] = &[
                ("tb", 1024.0 * 1024.0 * 1024.0),
                ("t",  1024.0 * 1024.0 * 1024.0),
                ("gb", 1024.0 * 1024.0 * 1024.0),
                ("g",  1024.0 * 1024.0 * 1024.0),
                ("mb", 1024.0 * 1024.0),
                ("m",  1024.0 * 1024.0),
                ("kb", 1024.0),
                ("k",  1024.0),
                ("b", 1.0)
            ];

            let target_memory_usage_str = target_memory_usage_str.to_lowercase();

            let mut target_memory_usage = target_memory_usage_str.parse::<u64>().ok();

            if target_memory_usage.is_none() {
                for (suffix, multiplier) in MULTIPLIERS {
                    if let Some(prefix) = target_memory_usage_str.strip_suffix(suffix)
                        && let Ok(value) = prefix.trim().parse::<f64>()
                    {
                        target_memory_usage = Some((value * multiplier).round() as u64);

                        break;
                    }
                }
            }

            let Some(target_memory_usage) = target_memory_usage else {
                anyhow::bail!("invalid target_memory_usage value");
            };

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
                fast_verify,
                no_verify_before_download,
                user_agent,
                proxy,
                output_format,
                cli.ascii
            )
        }
    }
}
