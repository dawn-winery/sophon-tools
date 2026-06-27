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
    #[command(subcommand)]
    command: CliCommands
}

#[derive(Debug, Subcommand)]
enum CliCommands {
    /// Perform Sophon API requests.
    #[command(subcommand)]
    Api(CliCommandList)
}

#[derive(Debug, Subcommand)]
enum CliCommandList {
    /// List information about available games.
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
    }
}

fn main() -> anyhow::Result<()> {
    let logger = tracing_subscriber::fmt::layer()
        .pretty()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            !metadata.target().contains("rustls")
                && !metadata.target().contains("reqwest")
                && !metadata.target().contains("h2")
                && !metadata.target().contains("hyper_util")
        }));

    tracing_subscriber::registry()
        .with(logger)
        .init();

    match Cli::parse().command {
        CliCommands::Api(CliCommandList::ListGames {
            region,
            launcher_id,
            output_format
        }) => list_games::run(region, launcher_id, output_format)
    }
}
