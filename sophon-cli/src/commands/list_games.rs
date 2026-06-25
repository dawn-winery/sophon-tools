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

use sophon_lib::api::SophonApi;

use super::{SophonRegion, OutputFormat, reqwest_client, find_game_name};

pub fn run(
    region: SophonRegion,
    launcher_id: Option<String>,
    user_agent: Option<String>,
    proxy: Option<String>,
    output_format: OutputFormat,
    ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let api = SophonApi::from(reqwest_client(user_agent, proxy)?.build()?);

    let future = api.fetch_games_branches_info(
        region.into(),
        launcher_id
    );

    let games_branches = runtime.block_on(future)
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
                "biz",
                "name",
                "version",
                "package",
                "password"
            ]);

            for game_branch in games_branches {
                let game_name = find_game_name(
                    &game_branch.game_id,
                    &game_branch.game_biz
                );

                table.add_row([
                    game_branch.game_id,
                    game_branch.game_biz,
                    game_name.unwrap_or("-").to_string(),
                    game_branch.version,
                    game_branch.package_id,
                    game_branch.password
                ]);
            }

            println!("{table}");
        }

        OutputFormat::Json => {
            println!("{}", serde_json::to_string(&serde_json::json!(
                games_branches.into_iter()
                    .map(|game_branch| {
                        serde_json::json!({
                            "game": {
                                "id": game_branch.game_id,
                                "biz": game_branch.game_biz
                            },
                            "package": {
                                "id": game_branch.package_id,
                                "branch": game_branch.branch,
                                "password": game_branch.password
                            },
                            "version": game_branch.version,
                            "diff_versions": game_branch.diff_versions
                        })
                    })
                    .collect::<Vec<_>>()
            ))?);
        }
    }

    Ok(())
}
