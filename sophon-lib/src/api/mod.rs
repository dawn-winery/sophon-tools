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

use tokio::sync::RwLock;

use serde_json::Value as Json;

pub mod game_branch;
pub mod game_versions_info;
pub mod game_configs;
// pub mod package_patch_info;
// pub mod package_download_info;
pub mod game;

use crate::region::SophonRegion;

use game_branch::SophonApiGameBranch;
use game_versions_info::SophonApiGameVersionsInfo;
use game_configs::SophonApiGameConfigs;
use game::SophonApiGame;

#[derive(Debug, thiserror::Error)]
pub enum SophonApiError {
    #[error("failed to perform http request: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("failed to deserialize json response: {0}")]
    Deserialize(#[from] serde_json::Error),

    #[error("sophon API returned invalid status: {code} {message}")]
    InvalidSophonStatus {
        code: i32,
        message: String
    },

    #[error("sophon API returned invalid response")]
    InvalidSophonResponse,

    #[error("sophon api with region '{region:?}' and launcher id '{launcher_id}' doesn't contain information about the game with id '{game_id}'")]
    GameNotFound {
        region: SophonRegion,
        launcher_id: String,
        game_id: String
    },

    #[error(transparent)]
    Other(Box<dyn std::error::Error>)
}

#[derive(Debug, serde::Deserialize)]
struct SophonApiResponse<T> {
    pub retcode: i32,
    pub message: String,
    pub data: Option<T>
}

#[derive(Default)]
struct GameCacheSlot<T> {
    pub region: SophonRegion,
    pub launcher_id: String,
    pub value: T
}

#[derive(Default)]
struct PackageCacheSlot<T> {
    pub region: SophonRegion,
    pub branch: String,
    pub password: String,
    pub package_id: String,
    pub version: String,
    pub value: T
}

pub struct SophonApi {
    client: reqwest::Client,

    game_branches_timeout: Duration,
    game_versions_info_timeout: Duration,
    game_configs_timeout: Duration,

    package_patch_info_timeout: Duration,
    package_download_info_timeout: Duration,

    game_branches_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameBranch]>>>>,
    game_versions_info_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameVersionsInfo]>>>>,
    game_configs_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameConfigs]>>>>,

    package_patch_info_cache: RwLock<Vec<PackageCacheSlot<()>>>,
    package_download_info_cache: RwLock<Vec<PackageCacheSlot<()>>>
}

impl Default for SophonApi {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/{}", crate::VERSION))
                .build()
                .expect("failed to build reqwest client"),

            game_branches_timeout: Duration::from_secs(5),
            game_versions_info_timeout: Duration::from_secs(5),
            game_configs_timeout: Duration::from_secs(5),

            package_patch_info_timeout: Duration::from_secs(5),
            package_download_info_timeout: Duration::from_secs(5),

            game_branches_cache: Default::default(),
            game_versions_info_cache: Default::default(),
            game_configs_cache: Default::default(),

            package_patch_info_cache: Default::default(),
            package_download_info_cache: Default::default()
        }
    }
}

impl From<reqwest::Client> for SophonApi {
    fn from(client: reqwest::Client) -> Self {
        Self {
            client,
            ..Self::default()
        }
    }
}

impl From<SophonApi> for reqwest::Client {
    #[inline]
    fn from(value: SophonApi) -> Self {
        value.client
    }
}

impl SophonApi {
    pub fn with_game_branches_timeout(mut self, timeout: Duration) -> Self {
        self.game_branches_timeout = timeout;

        self
    }

    pub fn with_game_versions_info_timeout(mut self, timeout: Duration) -> Self {
        self.game_versions_info_timeout = timeout;

        self
    }

    pub fn with_game_configs_timeout(mut self, timeout: Duration) -> Self {
        self.game_configs_timeout = timeout;

        self
    }

    pub fn with_package_patch_info_timeout(mut self, timeout: Duration) -> Self {
        self.package_patch_info_timeout = timeout;

        self
    }

    pub fn with_package_download_info_timeout(mut self, timeout: Duration) -> Self {
        self.package_download_info_timeout = timeout;

        self
    }

    /// Try to fetch list of available games, their versions and components.
    ///
    /// This information can be used to list information about game components,
    /// check latest available game version and whether it's possible to update
    /// from another version to it.
    ///
    /// `<game_info_url>/hyp/hyp-connect/api/getGameBranches`.
    pub async fn game_branches(
        &self,
        region: SophonRegion,
        launcher_id: Option<String>
    ) -> Result<Box<[SophonApiGameBranch]>, SophonApiError> {
        let launcher_id = launcher_id.unwrap_or_else(|| {
            region.launcher_id().to_string()
        });

        if let Some(slot) = self.game_branches_cache.read().await.iter()
            .find(|slot| {
                slot.region == region && slot.launcher_id == launcher_id
            })
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?region,
                ?launcher_id,
                "game_branches API cache read"
            );

            return Ok(slot.value.clone());
        }

        let url = format!(
            "{}/hyp/hyp-connect/api/getGameBranches?launcher_id={}",
            region.game_info_url(),
            launcher_id
        );

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?region,
            ?launcher_id,
            ?url,
            "fetch game_branches from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_branches_timeout)
            .send()
            .await?;

        let response = serde_json::from_slice::<SophonApiResponse<Json>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.retcode,
                message: response.message
            });
        };

        let Some(game_branches) = response.get("game_branches")
            .and_then(Json::as_array)
        else {
            return Err(SophonApiError::InvalidSophonResponse);
        };

        let game_branches = game_branches.iter()
            .map(|game_branch| {
                SophonApiGameBranch::try_from(game_branch)
                    .map_err(|err| SophonApiError::Other(err.into()))
            })
            .collect::<Result<Box<[_]>, SophonApiError>>()?;

        self.game_branches_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: game_branches.clone()
        });

        Ok(game_branches)
    }

    /// Try to fetch list all the versions of available games.
    ///
    /// This information can be used to detect currently installed game version.
    ///
    /// `<game_info_url>/hyp/hyp-connect/api/getGameScanInfo`.
    pub async fn game_versions_info(
        &self,
        region: SophonRegion,
        launcher_id: Option<String>
    ) -> Result<Box<[SophonApiGameVersionsInfo]>, SophonApiError> {
        let launcher_id = launcher_id.unwrap_or_else(|| {
            region.launcher_id().to_string()
        });

        if let Some(slot) = self.game_versions_info_cache.read().await.iter()
            .find(|slot| {
                slot.region == region && slot.launcher_id == launcher_id
            })
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?region,
                ?launcher_id,
                "game_versions_info API cache read"
            );

            return Ok(slot.value.clone());
        }

        let url = format!(
            "{}/hyp/hyp-connect/api/getGameScanInfo?launcher_id={}",
            region.game_info_url(),
            launcher_id
        );

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?region,
            ?launcher_id,
            ?url,
            "fetch game_versions_info from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_versions_info_timeout)
            .send()
            .await?;

        let response = serde_json::from_slice::<SophonApiResponse<Json>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.retcode,
                message: response.message
            });
        };

        let Some(versions_info) = response.get("game_scan_info")
            .and_then(Json::as_array)
        else {
            return Err(SophonApiError::InvalidSophonResponse);
        };

        let versions_info = versions_info.iter()
            .map(|versions_info| {
                SophonApiGameVersionsInfo::try_from(versions_info)
                    .map_err(|err| SophonApiError::Other(err.into()))
            })
            .collect::<Result<Box<[_]>, SophonApiError>>()?;

        self.game_versions_info_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: versions_info.clone()
        });

        Ok(versions_info)
    }

    /// Try to fetch paths information about available games.
    ///
    /// This information can be used to determine directories layout of the
    /// installed game files.
    ///
    /// `<game_info_url>/hyp/hyp-connect/api/getGameConfigs`.
    pub async fn game_configs(
        &self,
        region: SophonRegion,
        launcher_id: Option<String>
    ) -> Result<Box<[SophonApiGameConfigs]>, SophonApiError> {
        let launcher_id = launcher_id.unwrap_or_else(|| {
            region.launcher_id().to_string()
        });

        if let Some(slot) = self.game_configs_cache.read().await.iter()
            .find(|slot| {
                slot.region == region && slot.launcher_id == launcher_id
            })
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?region,
                ?launcher_id,
                "game_configs API cache read"
            );

            return Ok(slot.value.clone());
        }

        let url = format!(
            "{}/hyp/hyp-connect/api/getGameConfigs?launcher_id={}",
            region.game_info_url(),
            launcher_id
        );

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?region,
            ?launcher_id,
            ?url,
            "fetch game_configs from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_configs_timeout)
            .send()
            .await?;

        let response = serde_json::from_slice::<SophonApiResponse<Json>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.retcode,
                message: response.message
            });
        };

        let Some(game_configs) = response.get("launch_configs")
            .and_then(Json::as_array)
        else {
            return Err(SophonApiError::InvalidSophonResponse);
        };

        let game_configs = game_configs.iter()
            .map(|game_config| {
                SophonApiGameConfigs::try_from(game_config)
                    .map_err(|err| SophonApiError::Other(err.into()))
            })
            .collect::<Result<Box<[_]>, SophonApiError>>()?;

        self.game_configs_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: game_configs.clone()
        });

        Ok(game_configs)
    }

    /// Try to fetch game files patching information.
    ///
    /// `<sophon_data_url>/downloader/sophon_chunk/api/getPatchBuild`.
    pub async fn package_patch_info(
        &self,
        region: SophonRegion,
        branch: String,
        password: String,
        package_id: String,
        version: String
    ) -> Result<(), SophonApiError> {
        if let Some(slot) = self.package_patch_info_cache.read().await.iter()
            .find(|slot| {
                slot.region == region
                    && slot.branch == branch
                    && slot.password == password
                    && slot.package_id == package_id
                    && slot.version == version
            })
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?region,
                ?branch,
                ?password,
                ?package_id,
                ?version,
                "package_patch_info API cache read"
            );

            return Ok(slot.value.clone());
        }

        let url = format!(
            "{}/downloader/sophon_chunk/api/getPatchBuild?branch={}&password={}&package_id={}&tag={}",
            region.sophon_data_url(),
            branch,
            password,
            package_id,
            version
        );

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?region,
            ?branch,
            ?password,
            ?package_id,
            ?version,
            "fetch package_patch_info from the API"
        );

        let response = self.client.post(url)
            .timeout(self.game_configs_timeout)
            .send()
            .await?;

        let response = serde_json::from_slice::<SophonApiResponse<Json>>(
            &response.bytes().await?
        )?;

        std::fs::write("patch_info.json", serde_json::to_vec_pretty(&response.data)?).unwrap();

        todo!()

        // let response = serde_json::from_slice::<SophonApiResponse<Json>>(
        //     &response.bytes().await?
        // )?;

        // let Some(response) = response.data else {
        //     return Err(SophonApiError::InvalidSophonStatus {
        //         code: response.retcode,
        //         message: response.message
        //     });
        // };

        // let Some(game_configs) = response.get("launch_configs")
        //     .and_then(Json::as_array)
        // else {
        //     return Err(SophonApiError::InvalidSophonResponse);
        // };

        // let game_configs = game_configs.iter()
        //     .map(|game_config| {
        //         SophonApiGameConfigs::try_from(game_config)
        //             .map_err(|err| SophonApiError::Other(err.into()))
        //     })
        //     .collect::<Result<Box<[_]>, SophonApiError>>()?;

        // self.game_configs_cache.replace(GameCacheSlot {
        //     region,
        //     launcher_id,
        //     value: Some(game_configs.clone())
        // });

        // Ok(game_configs)
    }

    /// Get game info wrapper.
    pub fn game(
        &self,
        region: SophonRegion,
        launcher_id: Option<String>,
        game_id: String
    ) -> SophonApiGame<'_> {
        let launcher_id = launcher_id.unwrap_or_else(|| {
            region.launcher_id().to_string()
        });

        SophonApiGame::new(
            self,
            region,
            launcher_id,
            game_id
        )
    }
}

#[test]
fn test() {
    let runtime = tokio::runtime::Runtime::new()
        .unwrap();

    let api = SophonApi::default();

    runtime.block_on(async move {
        // let branches = api.game_branches(SophonRegion::Global, None).await;

        // dbg!(branches);

        // let versions = api.game_versions_info(SophonRegion::Global, None).await;

        // dbg!(versions);

        // let configs = api.game_configs(SophonRegion::Global, None).await;

        // dbg!(configs);

        let game = api.game(
            SophonRegion::Global,
            None,
            String::from("U5hbdsT9W7")
        );

        let branch = game.fetch_branch().await.unwrap();

        api.package_patch_info(
            SophonRegion::Global,
            branch.branch,
            branch.password,
            branch.package_id,
            branch.version
        ).await;
    });
}
