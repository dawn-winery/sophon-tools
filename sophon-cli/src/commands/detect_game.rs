// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@dawn.wine>
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

use sophon_lib::region::SophonRegion;
use sophon_lib::api::SophonApiError;

use crate::args::*;
use crate::commands::*;

pub fn run(
    game_dir: PathBuf,
    api_client: SophonApiClientArgs,
    output_format: OutputFormat,
    ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Build the reqwest client inside the runtime context.
    let guard = runtime.enter();
    let api = api_client.build()?;

    drop(guard);

    let mut detected_game = None;

    for region in [SophonRegion::Global, SophonRegion::China] {
        let games_branches = runtime.block_on(api.fetch_games_branches_info(region, None))
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

        for game_branch in games_branches.iter() {
            let game = api.game(region, None, game_branch.game.game_id.clone());

            match runtime.block_on(game.detect_version(&game_dir)) {
                Ok(Some(version)) => {
                    detected_game = Some((game, version));

                    break;
                }

                Ok(None) => continue,
                Err(SophonApiError::GameNotFound { .. }) => continue,

                Err(err) => anyhow::bail!(err.to_string())
            }
        }
    }

    let Some((game, version)) = detected_game else {
        return Ok(());
    };

    let game_configs = runtime.block_on(game.fetch_configs())
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    match output_format {
        OutputFormat::Text => {
            let mut table = comfy_table::Table::new();

            if ascii {
                table.load_style(comfy_table::presets::ASCII_FULL);
            } else {
                table.load_style(comfy_table::presets::UTF8_FULL);
            }

            table.set_content_arrangement(
                comfy_table::ContentArrangement::Dynamic
            );

            table.set_header(["field", "value"]);

            table.add_row(["id", game.game_id()]);
            table.add_row(["biz", &game_configs.game.game_biz]);
            table.add_row([
                "name",
                find_game_name(game.game_id(), &game_configs.game.game_biz)
                    .unwrap_or("-")
            ]);
            table.add_row(["version", &version]);
            table.add_row(["binary", &game_configs.binary_name]);

            println!("{table}");
        }

        OutputFormat::Json => {
            println!("{}", serde_json::to_string(&serde_json::json!({
                "game": {
                    "id": game.game_id(),
                    "biz": game_configs.game.game_biz
                },
                "version": version,
                "binary": game_configs.binary_name
            }))?);
        }
    }

    Ok(())
}
