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

use std::time::Duration;

use sophon_lib::api::SophonApi;

use super::*;

#[allow(clippy::too_many_arguments)]
pub fn run(
    game_id: String,
    region: SophonRegion,
    launcher_id: Option<String>,
    user_agent: Option<String>,
    proxy: Option<String>,
    timeout: Option<Duration>,
    output_format: OutputFormat,
    ascii: bool
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let api = SophonApi::from(reqwest_client(user_agent, proxy)?.build()?)
        .with_timeout_all(timeout.unwrap_or(Duration::MAX));

    let game = api.game(region.into(), launcher_id, game_id);

    let game_versions = runtime.block_on(game.fetch_versions_info())
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
                "version",
                "md5"
            ]);

            for game_version in game_versions.versions {
                table.add_row([
                    game_version.version,
                    game_version.hash_md5
                ]);
            }

            println!("{table}");
        }

        OutputFormat::Json => {
            println!("{}", serde_json::to_string(&serde_json::json!(
                game_versions.versions.into_iter()
                    .map(|game_version| {
                        serde_json::json!({
                            "version": game_version.version,
                            "hash_md5": game_version.hash_md5
                        })
                    })
                    .collect::<Vec<_>>()
            ))?);
        }
    }

    Ok(())
}
