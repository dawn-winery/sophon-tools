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
pub mod sophon_manifest_info;
pub mod sophon_download_info;
pub mod package_download_info;
pub mod package_update_info;
pub mod game;
pub mod package;

use crate::region::SophonRegion;

use game_branch::SophonApiGameBranch;
use game_versions_info::SophonApiGameVersionsInfo;
use game_configs::SophonApiGameConfigs;
use package_download_info::SophonApiPackageDownloadInfo;
use package_update_info::SophonApiPackageUpdateInfo;
use game::SophonApiGame;

#[derive(Debug, thiserror::Error)]
pub enum SophonApiError {
    #[error("failed to perform http request: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("failed to deserialize json response: {0}")]
    Deserialize(#[from] serde_json::Error),

    #[error("failed to perform io operation: {0}")]
    Io(#[from] std::io::Error),

    #[error("sophon API returned invalid status: {code} {message}")]
    InvalidSophonStatus {
        code: i64,
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

#[derive(Debug)]
struct SophonApiResponse<'response> {
    pub code: i64,
    pub message: String,
    pub data: Option<&'response Json>
}

impl<'response> TryFrom<&'response Json> for SophonApiResponse<'response> {
    type Error = SophonApiError;

    fn try_from(value: &'response Json) -> Result<Self, Self::Error> {
        Ok(Self {
            code: value.get("retcode")
                .and_then(Json::as_i64)
                .ok_or(SophonApiError::InvalidSophonResponse)?,

            message: value.get("message")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiError::InvalidSophonResponse)?,

            data: value.get("data")
        })
    }
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

    game_branches_timeout: Option<Duration>,
    game_versions_info_timeout: Option<Duration>,
    game_configs_timeout: Option<Duration>,

    package_download_info_timeout: Option<Duration>,
    package_update_info_timeout: Option<Duration>,

    game_branches_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameBranch]>>>>,
    game_versions_info_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameVersionsInfo]>>>>,
    game_configs_cache: RwLock<Vec<GameCacheSlot<Box<[SophonApiGameConfigs]>>>>,

    package_download_info_cache: RwLock<Vec<PackageCacheSlot<SophonApiPackageDownloadInfo>>>,
    package_update_info_cache: RwLock<Vec<PackageCacheSlot<SophonApiPackageUpdateInfo>>>
}

impl Default for SophonApi {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/v{}", crate::VERSION))
                .deflate(true)
                .gzip(true)
                .build()
                .expect("failed to build reqwest client"),

            game_branches_timeout: None,
            game_versions_info_timeout: None,
            game_configs_timeout: None,

            package_download_info_timeout: None,
            package_update_info_timeout: None,

            game_branches_cache: RwLock::const_new(Vec::with_capacity(1)),
            game_versions_info_cache: RwLock::const_new(Vec::with_capacity(1)),
            game_configs_cache: RwLock::const_new(Vec::with_capacity(1)),

            package_download_info_cache: RwLock::const_new(Vec::with_capacity(1)),
            package_update_info_cache: RwLock::const_new(Vec::with_capacity(1))
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
        self.game_branches_timeout = Some(timeout);

        self
    }

    pub fn with_game_versions_info_timeout(mut self, timeout: Duration) -> Self {
        self.game_versions_info_timeout = Some(timeout);

        self
    }

    pub fn with_game_configs_timeout(mut self, timeout: Duration) -> Self {
        self.game_configs_timeout = Some(timeout);

        self
    }

    pub fn with_package_download_info_timeout(mut self, timeout: Duration) -> Self {
        self.package_download_info_timeout = Some(timeout);

        self
    }

    pub fn with_package_update_info_timeout(mut self, timeout: Duration) -> Self {
        self.package_update_info_timeout = Some(timeout);

        self
    }

    #[inline]
    pub const fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Try to fetch list of available games, their versions and components.
    ///
    /// This information can be used to list information about game components,
    /// check latest available game version and whether it's possible to update
    /// from another version to it.
    ///
    /// `<game_info_url>/hyp/hyp-connect/api/getGameBranches`.
    pub async fn fetch_games_branches_info(
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
                "games_branches API cache read"
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
            "fetch games_branches from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_branches_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let response = serde_json::from_slice::<Json>(
            &response.bytes().await?
        )?;

        let response = SophonApiResponse::try_from(&response)?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
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
    pub async fn fetch_games_versions_info(
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
                "games_versions_info API cache read"
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
            "fetch games_versions_info from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_versions_info_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let response = serde_json::from_slice::<Json>(
            &response.bytes().await?
        )?;

        let response = SophonApiResponse::try_from(&response)?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
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
    pub async fn fetch_games_configs(
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
                "games_configs API cache read"
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
            "fetch games_configs from the API"
        );

        let response = self.client.get(url)
            .timeout(self.game_configs_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let response = serde_json::from_slice::<Json>(
            &response.bytes().await?
        )?;

        let response = SophonApiResponse::try_from(&response)?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
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

    /// Try to fetch game files downloading information.
    ///
    /// This information can be used to download game or game components files
    /// of specific version.
    ///
    /// `<sophon_data_url>/downloader/sophon_chunk/api/getBuild`.
    pub async fn fetch_package_download_info(
        &self,
        region: SophonRegion,
        branch: String,
        password: String,
        package_id: String,
        version: String
    ) -> Result<SophonApiPackageDownloadInfo, SophonApiError> {
        if let Some(slot) = self.package_download_info_cache.read().await.iter()
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
                "package_download_info API cache read"
            );

            return Ok(slot.value.clone());
        }

        let url = format!(
            "{}/downloader/sophon_chunk/api/getBuild?branch={}&password={}&package_id={}&tag={}",
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
            ?url,
            "fetch package_download_info from the API"
        );

        let response = self.client.get(url)
            .timeout(self.package_download_info_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let response = serde_json::from_slice::<Json>(
            &response.bytes().await?
        )?;

        let response = SophonApiResponse::try_from(&response)?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let download_info = SophonApiPackageDownloadInfo::try_from(response)
            .map_err(|err| SophonApiError::Other(err.into()))?;

        self.package_download_info_cache.write().await.push(PackageCacheSlot {
            region,
            branch,
            password,
            package_id,
            version,
            value: download_info.clone()
        });

        Ok(download_info)
    }

    /// Try to fetch game files updating information.
    ///
    /// This information can be used to update game or game components files
    /// from the given version to the latest available one.
    ///
    /// `<sophon_data_url>/downloader/sophon_chunk/api/getPatchBuild`.
    pub async fn fetch_package_update_info(
        &self,
        region: SophonRegion,
        branch: String,
        password: String,
        package_id: String,
        version: String
    ) -> Result<SophonApiPackageUpdateInfo, SophonApiError> {
        if let Some(slot) = self.package_update_info_cache.read().await.iter()
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
                "package_update_info API cache read"
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
            ?url,
            "fetch package_update_info from the API"
        );

        let response = self.client.post(url)
            .timeout(self.package_update_info_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let response = serde_json::from_slice::<Json>(
            &response.bytes().await?
        )?;

        let response = SophonApiResponse::try_from(&response)?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let package_info = SophonApiPackageUpdateInfo::try_from(response)
            .map_err(|err| SophonApiError::Other(err.into()))?;

        self.package_update_info_cache.write().await.push(PackageCacheSlot {
            region,
            branch,
            password,
            package_id,
            version,
            value: package_info.clone()
        });

        Ok(package_info)
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
