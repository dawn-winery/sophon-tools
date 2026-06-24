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

use sophon_lib::region::SophonRegion;
use sophon_lib::api::SophonApi;

use crate::commands::find_game_name;

use super::OutputFormat;

pub fn run(
    path: PathBuf,
    output_format: OutputFormat,
    ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let api = SophonApi::default();

    let mut detected_game = None;

    for region in [SophonRegion::Global, SophonRegion::China] {
        let game_configs = runtime.block_on(api.fetch_games_configs(region, None))
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

        for game_config in game_configs {
            if path.join(game_config.binary_name).is_file() {
                let game = api.game(region, None, game_config.game_id);

                let version = runtime.block_on(game.detect_version(&path))
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;

                if let Some(version) = version {
                    detected_game = Some((game, version));

                    break;
                }
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
                table.load_preset(comfy_table::presets::ASCII_FULL);
            } else {
                table.load_preset(comfy_table::presets::UTF8_FULL);
            }

            table.set_content_arrangement(
                comfy_table::ContentArrangement::Dynamic
            );

            table.set_header(["field", "value"]);

            table.add_row(["id", game.game_id()]);
            table.add_row(["biz", &game_configs.game_biz]);
            table.add_row([
                "name",
                find_game_name(game.game_id(), &game_configs.game_biz)
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
                    "biz": game_configs.game_biz
                },
                "version": version,
                "binary": game_configs.binary_name
            }))?);
        }
    }

    Ok(())
}
