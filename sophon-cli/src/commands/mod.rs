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

use clap::ValueEnum;

pub mod list_games;
pub mod list_components;
pub mod game_versions;
pub mod download_info;
pub mod verify_game;
pub mod download_game;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SophonRegion {
    #[value(name = "global")]
    Global,

    #[value(name = "china")]
    China
}

impl From<SophonRegion> for sophon_lib::region::SophonRegion {
    fn from(value: SophonRegion) -> Self {
        match value {
            SophonRegion::Global => Self::Global,
            SophonRegion::China => Self::China
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    #[value(name = "text")]
    Text,

    #[value(name = "json")]
    Json
}
