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
pub enum SophonApiGameBranchError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiGameBranch {
    /// `game.id` - unique identifier of the game.
    ///
    /// Example: `U5hbdsT9W7`
    pub game_id: String,

    /// `game.biz` - human-readable identifier of the game. May not be unique.
    ///
    /// Example: `nap_global`
    pub game_biz: String,

    /// `main.package_id`
    ///
    /// Example: `qsoIbfMm4x`
    pub package_id: String,

    /// `main.branch`. Always "main" on the public API endpoint.
    ///
    /// Example: `main`
    pub branch: String,

    /// `main.password`
    ///
    /// Example: `lh0OmNjbhG9x`
    pub password: String,

    /// `main.tag` - latest available version of the game.
    ///
    /// Example: `3.0.0`
    pub version: String,

    /// `main.diff_tags` - list of versions from which the game can be updated.
    ///
    /// Example: `["2.8.0", "2.7.0"]`
    pub diff_versions: Box<[String]>,

    /// `main.categories` - list of game components. Can be base game files,
    /// voice packages, etc.
    pub categories: Box<[SophonApiGameBranchCategory]>
}

impl TryFrom<&Json> for SophonApiGameBranch {
    type Error = SophonApiGameBranchError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        let Some(game) = value.get("game") else {
            return Err(SophonApiGameBranchError::InvalidField("game"));
        };

        let Some(main) = value.get("main") else {
            return Err(SophonApiGameBranchError::InvalidField("main"));
        };

        Ok(Self {
            game_id: game.get("id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("game.id"))?,

            game_biz: game.get("biz")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("game.biz"))?,

            package_id: main.get("package_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.package_id"))?,

            branch: main.get("branch")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.branch"))?,

            password: main.get("password")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.password"))?,

            version: main.get("tag")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.tag"))?,

            diff_versions: main.get("diff_tags")
                .and_then(Json::as_array)
                .and_then(|tags| {
                    tags.iter()
                        .map(|tag| tag.as_str().map(String::from))
                        .collect::<Option<Box<[String]>>>()
                })
                .ok_or(SophonApiGameBranchError::InvalidField("main.diff_tags"))?,

            categories: main.get("categories")
                .and_then(Json::as_array)
                .ok_or(SophonApiGameBranchError::InvalidField("main.categories"))
                .and_then(|categories| {
                    categories.iter()
                        .map(SophonApiGameBranchCategory::try_from)
                        .collect::<Result<Box<[_]>, SophonApiGameBranchError>>()
                })?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiGameBranchCategory {
    /// `main.categories[].category_id` - per-game category identifier.
    ///
    /// Example: `10176`
    pub id: String,

    /// `main.categories[].matching_field` - name of the category.
    ///
    /// Example: `game`
    pub name: String
}

impl TryFrom<&Json> for SophonApiGameBranchCategory {
    type Error = SophonApiGameBranchError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.get("category_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.categories[].category_id"))?,

            name: value.get("matching_field")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiGameBranchError::InvalidField("main.categories[].matching_field"))?
        })
    }
}
