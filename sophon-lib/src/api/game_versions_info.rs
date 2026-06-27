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
pub enum SophonApiGameVersionsInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiGameVersionsInfo {
    /// `game_id` - unique identifier of the game.
    ///
    /// Example: `U5hbdsT9W7`
    pub game_id: String,

    /// `game_exe_list` - list of all the game versions and md5 hashes of the
    /// game binaries for these versions.
    pub versions: Box<[SophonApiGameVersionInfo]>
}

impl TryFrom<&Json> for SophonApiGameVersionsInfo {
    type Error = SophonApiGameVersionsInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            game_id: value.get("game_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameVersionsInfoError::InvalidField("game_id"))?,

            versions: value.get("game_exe_list")
                .and_then(Json::as_array)
                .ok_or(SophonApiGameVersionsInfoError::InvalidField("game_exe_list"))
                .and_then(|categories| {
                    categories.iter()
                        .map(SophonApiGameVersionInfo::try_from)
                        .collect::<Result<Box<[_]>, SophonApiGameVersionsInfoError>>()
                })?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiGameVersionInfo {
    /// `game_exe_list[].version` - version of the game.
    ///
    /// Example: `3.0.0`
    pub version: String,

    /// `game_exe_list[].md5` - md5 hash of the game binary.
    ///
    /// Example: `eb8ee0dd5c5a7dd9aec6569182886539`
    pub md5: String
}

impl TryFrom<&Json> for SophonApiGameVersionInfo {
    type Error = SophonApiGameVersionsInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            version: value.get("version")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameVersionsInfoError::InvalidField("game_exe_list[].version"))?,

            md5: value.get("md5")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameVersionsInfoError::InvalidField("game_exe_list[].md5"))?
        })
    }
}
