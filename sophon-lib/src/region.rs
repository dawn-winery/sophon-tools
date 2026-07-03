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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SophonRegion {
    #[default]
    Global,
    China
}

impl SophonRegion {
    /// Get base URL to the API related to the information about available
    /// games.
    ///
    /// Example endpoints:
    ///
    /// - `<url>/hyp/hyp-connect/api/getGameScanInfo`
    /// - `<url>/hyp/hyp-connect/api/getGameConfigs`
    pub const fn game_info_url(&self) -> &str {
        match self {
            Self::Global => "https://sg-hyp-api.hoyoverse.com",
            Self::China => "https://hyp-api.mihoyo.com"
        }
    }

    /// Get base URL to the API related to the sophon chunks downloading.
    ///
    /// Example endpoints:
    ///
    /// - `<url>/downloader/sophon_chunk/api/getBuild`
    /// - `<url>/downloader/sophon_chunk/api/getPatchBuild`
    pub const fn sophon_data_url(&self) -> &str {
        match self {
            Self::Global => "https://sg-public-api.hoyoverse.com",
            Self::China => "https://api-takumi.mihoyo.com"
        }
    }

    /// Get default sophon launcher ID for the current region.
    pub const fn launcher_id(&self) -> &str {
        match self {
            Self::Global => "VYTpXlbWo8",
            Self::China => "jGHBHlcOq1"
        }
    }
}
