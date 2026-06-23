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
    Api(CliApiCommands)
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
        game_id: String,

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
        game_id: String,

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

    #[command(
        alias = "game-info",
        alias = "package-info",
        alias = "package-download-info",
        alias = "download-info"
    )]
    GameDownloadInfo {
        #[arg(index = 1, required = true)]
        game_id: String,

        #[arg(index = 2, default_value_t = String::from("game"))]
        component_id: String,

        #[arg(index = 3)]
        version: Option<String>,

        #[arg(
            long, value_enum, default_value_t = SophonRegion::Global,
            alias = "edition"
        )]
        region: SophonRegion,

        #[arg(long)]
        launcher_id: Option<String>,

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
            game_id,
            region,
            launcher_id,
            output_format,
            show_all
        }) => list_components::run(
            game_id,
            region,
            launcher_id,
            output_format,
            show_all,
            cli.ascii
        ),

        CliCommands::Api(CliApiCommands::GameVersions {
            game_id,
            region,
            launcher_id,
            output_format
        }) => game_versions::run(
            game_id,
            region,
            launcher_id,
            output_format,
            cli.ascii
        ),

        CliCommands::Api(CliApiCommands::GameDownloadInfo {
            game_id,
            component_id,
            version,
            region,
            launcher_id,
            output_format
        }) => download_info::run(
            game_id,
            component_id,
            version,
            region,
            launcher_id,
            output_format,
            cli.ascii
        )
    }
}
