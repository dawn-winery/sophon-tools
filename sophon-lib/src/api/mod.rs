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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

pub mod responses;
pub mod game;
pub mod package;

use crate::region::SophonRegion;

use responses::{
    SophonApiResponse,
    GamesBranchesResponse, GamePackageInfo,
    GameVersionsResponse, GameVersionsInfo,
    GamesConfigsResponse, GameConfigInfo,
    DownloadGameResponse,
    UpdateGameResponse
};
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

#[derive(Default)]
struct GameCacheSlot<T> {
    pub region: SophonRegion,
    pub launcher_id: String,
    pub value: Arc<T>
}

#[derive(Default)]
struct PackageCacheSlot<T> {
    pub region: SophonRegion,
    pub branch: String,
    pub password: String,
    pub package_id: String,
    pub version: String,
    pub value: Arc<T>
}

pub struct SophonApi {
    client: reqwest::Client,

    game_branches_timeout: Option<Duration>,
    game_versions_info_timeout: Option<Duration>,
    game_configs_timeout: Option<Duration>,

    package_download_info_timeout: Option<Duration>,
    package_update_info_timeout: Option<Duration>,

    game_branches_cache: RwLock<Vec<GameCacheSlot<Box<[GamePackageInfo]>>>>,
    game_versions_cache: RwLock<Vec<GameCacheSlot<Box<[GameVersionsInfo]>>>>,
    game_configs_cache: RwLock<Vec<GameCacheSlot<Box<[GameConfigInfo]>>>>,

    package_download_info_cache: RwLock<Vec<PackageCacheSlot<DownloadGameResponse>>>,
    package_update_info_cache: RwLock<Vec<PackageCacheSlot<UpdateGameResponse>>>
}

impl Default for SophonApi {
    fn default() -> Self {
        Self {
            client: crate::client_builder()
                .build()
                .expect("failed to build reqwest client"),

            game_branches_timeout: None,
            game_versions_info_timeout: None,
            game_configs_timeout: None,

            package_download_info_timeout: None,
            package_update_info_timeout: None,

            game_branches_cache: RwLock::const_new(Vec::with_capacity(1)),
            game_versions_cache: RwLock::const_new(Vec::with_capacity(1)),
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
    pub fn with_timeout_all(mut self, timeout: Duration) -> Self {
        self.game_branches_timeout = Some(timeout);
        self.game_versions_info_timeout = Some(timeout);
        self.game_configs_timeout = Some(timeout);
        self.package_download_info_timeout = Some(timeout);
        self.package_update_info_timeout = Some(timeout);

        self
    }

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
    ) -> Result<Arc<Box<[GamePackageInfo]>>, SophonApiError> {
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

        let response = serde_json::from_slice::<SophonApiResponse<GamesBranchesResponse>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let value = Arc::new(response.values);

        self.game_branches_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: value.clone()
        });

        Ok(value)
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
    ) -> Result<Arc<Box<[GameVersionsInfo]>>, SophonApiError> {
        let launcher_id = launcher_id.unwrap_or_else(|| {
            region.launcher_id().to_string()
        });

        if let Some(slot) = self.game_versions_cache.read().await.iter()
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

        let response = serde_json::from_slice::<SophonApiResponse<GameVersionsResponse>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let value = Arc::new(response.values);

        self.game_versions_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: value.clone()
        });

        Ok(value)
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
    ) -> Result<Arc<Box<[GameConfigInfo]>>, SophonApiError> {
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

        let response = serde_json::from_slice::<SophonApiResponse<GamesConfigsResponse>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let value = Arc::new(response.values);

        self.game_configs_cache.write().await.push(GameCacheSlot {
            region,
            launcher_id,
            value: value.clone()
        });

        Ok(value)
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
    ) -> Result<Arc<DownloadGameResponse>, SophonApiError> {
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

        let response = serde_json::from_slice::<SophonApiResponse<DownloadGameResponse>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let value = Arc::new(response);

        self.package_download_info_cache.write().await.push(PackageCacheSlot {
            region,
            branch,
            password,
            package_id,
            version,
            value: value.clone()
        });

        Ok(value)
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
    ) -> Result<Arc<UpdateGameResponse>, SophonApiError> {
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

        let response = serde_json::from_slice::<SophonApiResponse<UpdateGameResponse>>(
            &response.bytes().await?
        )?;

        let Some(response) = response.data else {
            return Err(SophonApiError::InvalidSophonStatus {
                code: response.code,
                message: response.message
            });
        };

        let value = Arc::new(response);

        self.package_update_info_cache.write().await.push(PackageCacheSlot {
            region,
            branch,
            password,
            package_id,
            version,
            value: value.clone()
        });

        Ok(value)
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
