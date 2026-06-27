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

use crate::args::*;
use crate::commands::*;

fn is_category_known(name: &str) -> bool {
    name.chars().any(|c| !c.is_numeric()) && name != "null"
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    api_args: SophonApiArgs,
    show_all: bool,
    api_client: SophonApiClientArgs,
    output_format: OutputFormat,
    ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let api = api_client.build()?;

    let game = api.game(
        api_args.region.into(),
        api_args.launcher_id,
        game_id
    );

    let game_branch = runtime.block_on(game.fetch_branch_info())
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

            table.set_header([
                "id",
                "name",
                "title"
            ]);

            for category in game_branch.branch.categories {
                if show_all || is_category_known(&category.name) {
                    table.add_row([
                        &category.id,
                        &category.name,
                        find_component_title(&category.name).unwrap_or("-")
                    ]);
                }
            }

            println!("{table}");
        }

        OutputFormat::Json => {
            println!("{}", serde_json::to_string(&serde_json::json!(
                game_branch.branch.categories.into_iter()
                    .filter(|category| {
                        show_all || is_category_known(&category.name)
                    })
                    .map(|category| {
                        serde_json::json!({
                            "id": category.id,
                            "name": category.name
                        })
                    })
                    .collect::<Vec<_>>()
            ))?);
        }
    }

    Ok(())
}
