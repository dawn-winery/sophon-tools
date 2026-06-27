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

use serde_json::Value as Json;

#[derive(Debug, Clone, thiserror::Error)]
pub enum SophonApiGameConfigsError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiGameConfigs {
    /// `game.id` - unique identifier of the game.
    ///
    /// Example: `U5hbdsT9W7`
    pub game_id: String,

    /// `game.biz` - human-readable identifier of the game. May not be unique.
    ///
    /// Example: `nap_global`
    pub game_biz: String,

    /// `installation_dir` - name of the directory in which the game should be
    /// installed.
    ///
    /// Example: `ZenlessZoneZero Game`
    pub directory_name: String,

    /// `exe_file_name` - name of the main game binary.
    ///
    /// Example: `ZenlessZoneZero.exe`
    pub binary_name: String
}

impl TryFrom<&Json> for SophonApiGameConfigs {
    type Error = SophonApiGameConfigsError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        let Some(game) = value.get("game") else {
            return Err(SophonApiGameConfigsError::InvalidField("game"));
        };

        Ok(Self {
            game_id: game.get("id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameConfigsError::InvalidField("game.id"))?,

            game_biz: game.get("biz")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameConfigsError::InvalidField("game.biz"))?,

            directory_name: value.get("installation_dir")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameConfigsError::InvalidField("installation_dir"))?,

            binary_name: value.get("exe_file_name")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameConfigsError::InvalidField("exe_file_name"))?
        })
    }
}
