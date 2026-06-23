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

use super::*;
use super::package::SophonApiPackage;

/// Wrapper around the `SophonApi` struct that allows you to easily read
/// information about the game.
pub struct SophonApiGame<'api> {
    api: &'api SophonApi,
    region: SophonRegion,
    launcher_id: String,
    game_id: String
}

impl<'api> SophonApiGame<'api> {
    pub const fn new(
        api: &'api SophonApi,
        region: SophonRegion,
        launcher_id: String,
        game_id: String
    ) -> Self {
        Self {
            api,
            region,
            launcher_id,
            game_id
        }
    }

    /// Try to find branch information about the current game.
    pub async fn fetch_branch_info(
        &self
    ) -> Result<SophonApiGameBranch, SophonApiError> {
        let game_branches = self.api.fetch_games_branches_info(
            self.region,
            Some(self.launcher_id.clone())
        ).await?;

        let Some(game_branch) = game_branches.into_iter()
            .find(|game_branch| game_branch.game_id == self.game_id)
        else {
            return Err(SophonApiError::GameNotFound {
                region: self.region,
                launcher_id: self.launcher_id.clone(),
                game_id: self.game_id.clone()
            });
        };

        Ok(game_branch)
    }

    /// Try to find versions info about the current game.
    pub async fn fetch_versions_info(
        &self
    ) -> Result<SophonApiGameVersionsInfo, SophonApiError> {
        let versions_info = self.api.fetch_games_versions_info(
            self.region,
            Some(self.launcher_id.clone())
        ).await?;

        let Some(version_info) = versions_info.into_iter()
            .find(|version_info| version_info.game_id == self.game_id)
        else {
            return Err(SophonApiError::GameNotFound {
                region: self.region,
                launcher_id: self.launcher_id.clone(),
                game_id: self.game_id.clone()
            });
        };

        Ok(version_info)
    }

    /// Try to find configs for the current game.
    pub async fn fetch_configs(
        &self
    ) -> Result<SophonApiGameConfigs, SophonApiError> {
        let game_configs = self.api.fetch_games_configs(
            self.region,
            Some(self.launcher_id.clone())
        ).await?;

        let Some(game_config) = game_configs.into_iter()
            .find(|game_config| game_config.game_id == self.game_id)
        else {
            return Err(SophonApiError::GameNotFound {
                region: self.region,
                launcher_id: self.launcher_id.clone(),
                game_id: self.game_id.clone()
            });
        };

        Ok(game_config)
    }

    /// Get package wrapper for the current game. If `version` is not provided,
    /// then the latest available game version is used.
    pub async fn package(
        &self,
        version: Option<String>
    ) -> Result<SophonApiPackage<'api>, SophonApiError> {
        let branch_info = self.fetch_branch_info().await?;

        Ok(SophonApiPackage::new(
            self.api,
            self.region,
            branch_info.branch,
            branch_info.password,
            branch_info.package_id,
            version.unwrap_or(branch_info.version)
        ))
    }

    /// Get latest available game version.
    pub async fn latest_version(&self) -> Result<String, SophonApiError> {
        Ok(self.fetch_branch_info().await?.version)
    }

    /// Get list of versions from which the game can be updated to the latest
    /// available version.
    pub async fn updatable_versions(
        &self
    ) -> Result<Box<[String]>, SophonApiError> {
        Ok(self.fetch_branch_info().await?.diff_versions)
    }
}
