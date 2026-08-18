// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@dawn.wine>
//                     "John the Cooling Fan" <ivan8215145640@gmail.com>
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

pub mod utils;
pub mod args;
pub mod commands;

use args::*;
use commands::*;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Enable trace logs in stderr.
    #[arg(
        long,
        alias = "log",
        alias = "trace",
        alias = "logs",
        alias = "traces",
        alias = "tracing",
        alias = "tracings"
    )]
    logs: bool,

    /// Disable unicode output.
    #[arg(long, alias = "ansi")]
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
        /// Path to the game installation directory.
        #[arg(index = 1, required = true)]
        game_dir: PathBuf,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Verify installed game files.
    Verify {
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        /// Path to the game installation directory.
        #[arg(index = 2, required = true)]
        game_dir: PathBuf,

        #[command(flatten)]
        api_args: SophonApiArgs,

        #[command(flatten)]
        component: SophonApiGameComponentArg,

        /// Version of the game or component to verify installed file against.
        ///
        /// If unset, the latest available game version will be used.
        #[arg(long)]
        version: Option<String>,

        /// Verify files that match the regex.
        #[arg(long, short('r'))]
        regex: Option<String>,

        /// Use files sizes for verification instead of calculating md5 hashes.
        #[arg(
            long, default_value_t = false,
            value_parser = clap::value_parser!(bool),
            action = ArgAction::Set,
            alias = "fast",
            alias = "fast-verifying"
        )]
        fast_verify: bool,

        /// Show missing files.
        ///
        /// If unset, only installed files will be verifier.
        ///
        /// If set, verifier will list all the files that are missing from the
        /// given game directory.
        #[arg(
            long, default_value_t = true,
            value_parser = clap::value_parser!(bool),
            action = ArgAction::Set,
            alias = "missing",
            alias = "find-missing"
        )]
        show_missing: bool,

        /// Amount of threads to use in the tokio async runtime.
        ///
        /// Majority of tasks are IO-bound and are executed asynchronously.
        /// Setting this value higher than default value will not improve
        /// verification speed.
        #[arg(
            long, short('t'),
            alias = "workers",
            default_value_t = std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1)
        )]
        threads: usize,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Download game.
    Download {
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        /// Path to the game installation directory.
        #[arg(index = 2, required = true)]
        game_dir: PathBuf,

        #[command(flatten)]
        api_args: SophonApiArgs,

        #[command(flatten)]
        component: SophonApiGameComponentArg,

        /// Version of the game or component to download.
        ///
        /// If unset, the latest available game version will be used.
        #[arg(long)]
        version: Option<String>,

        /// Download files that match the regex.
        #[arg(long, short('r'))]
        regex: Option<String>,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[command(flatten)]
        downloader: SophonDownloaderArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Update game.
    Update {
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        /// Path to the game installation directory.
        #[arg(index = 2, required = true)]
        game_dir: PathBuf,

        #[command(flatten)]
        api_args: SophonApiArgs,

        #[command(flatten)]
        component: SophonApiGameComponentArg,

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

        /// Path to the chunks downloading directory.
        #[arg(
            long, default_value = ".cache",
            alias = "chunks-path",
            alias = "download-dir",
            alias = "download-path",
            alias = "chunks-download-dir",
            alias = "chunks-download-path"
        )]
        chunks_dir: PathBuf,

        /// Update files that match the regex.
        #[arg(long, short('r'))]
        regex: Option<String>,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[command(flatten)]
        updater: SophonUpdaterArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    }
}

#[derive(Debug, Subcommand)]
enum CliApiCommands {
    /// List information about available games.
    #[command(alias = "games")]
    ListGames {
        #[command(flatten)]
        api_args: SophonApiArgs,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

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
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        #[command(flatten)]
        api_args: SophonApiArgs,

        /// Show all non-standard components.
        #[arg(long)]
        show_all: bool,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Get list of all the game versions.
    #[command(
        alias = "versions",
        alias = "list-versions",
        alias = "list-game-versions"
    )]
    GameVersions {
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        #[command(flatten)]
        api_args: SophonApiArgs,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat
    },

    /// Get download information of the given game version and component.
    #[command(
        alias = "game-info",
        alias = "package-info",
        alias = "package-download-info",
        alias = "download-info",
        alias = "get-game-download-info",
        alias = "get-game-info",
        alias = "get-package-download-info",
        alias = "get-package-info",
        alias = "get-download-info",
        alias = "list-game-download-info",
        alias = "list-game-info",
        alias = "list-package-download-info",
        alias = "list-package-info",
        alias = "list-download-info"
    )]
    GameDownloadInfo {
        /// Game name or identifier.
        #[arg(index = 1, required = true)]
        game_id: String,

        /// Component name or identifier.
        #[arg(index = 2, default_value_t = String::from("game"))]
        component_id: String,

        /// Game version.
        ///
        /// If unset, the latest one is used.
        #[arg(index = 3)]
        version: Option<String>,

        #[command(flatten)]
        api_args: SophonApiArgs,

        /// Show files that match the regex.
        #[arg(long, short('r'))]
        regex: Option<String>,

        #[command(flatten)]
        api_client: SophonApiClientArgs,

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
            api_args,
            api_client,
            output_format
        }) => {
            list_games::run(
                api_args,
                api_client,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::ListComponents {
            game_id,
            api_args,
            show_all,
            api_client,
            output_format
        }) => {
            list_components::run(
                game_id,
                api_args,
                show_all,
                api_client,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::GameVersions {
            game_id,
            api_args,
            api_client,
            output_format
        }) => {
            game_versions::run(
                game_id,
                api_args,
                api_client,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Api(CliApiCommands::GameDownloadInfo {
            game_id,
            component_id,
            version,
            api_args,
            regex,
            api_client,
            output_format
        }) => {
            download_info::run(
                game_id,
                component_id,
                version,
                api_args,
                regex,
                api_client,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Detect {
            game_dir,
            api_client,
            output_format
        } => {
            detect_game::run(
                game_dir,
                api_client,
                output_format,
                cli.ascii
            )
        }

        CliCommands::Verify {
            game_id,
            game_dir,
            api_args,
            component,
            version,
            regex,
            fast_verify,
            show_missing,
            threads,
            api_client,
            output_format
        } => {
            verify_game::run(
                game_id,
                game_dir,
                api_args,
                component,
                version,
                regex,
                fast_verify,
                show_missing,
                threads,
                api_client,
                output_format
            )
        }

        CliCommands::Download {
            game_id,
            game_dir,
            api_args,
            component,
            version,
            regex,
            api_client,
            downloader,
            output_format
        } => {
            download_game::run(
                game_id,
                game_dir,
                api_args,
                component,
                version,
                regex,
                api_client,
                downloader,
                output_format
            )
        }

        CliCommands::Update {
            game_id,
            game_dir,
            api_args,
            component,
            from_version,
            to_version,
            chunks_dir,
            regex,
            api_client,
            updater,
            output_format
        } => {
            update_game::run(
                game_id,
                game_dir,
                api_args,
                component,
                from_version,
                to_version,
                chunks_dir,
                regex,
                api_client,
                updater,
                output_format
            )
        }
    }
}
