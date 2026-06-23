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

/// Wrapper around the `SophonApi` struct that allows you to easily read
/// information about the game package (branch).
pub struct SophonApiPackage<'api> {
    api: &'api SophonApi,
    region: SophonRegion,
    branch: String,
    password: String,
    package_id: String,
    version: String
}

impl<'api> SophonApiPackage<'api> {
    pub const fn new(
        api: &'api SophonApi,
        region: SophonRegion,
        branch: String,
        password: String,
        package_id: String,
        version: String
    ) -> Self {
        Self {
            api,
            region,
            branch,
            password,
            package_id,
            version
        }
    }

    /// Fetch current game package files download info.
    pub async fn fetch_download_info(
        &self
    ) -> Result<SophonApiPackageDownloadInfo, SophonApiError> {
        self.api.fetch_package_download_info(
            self.region,
            self.branch.clone(),
            self.password.clone(),
            self.package_id.clone(),
            self.version.clone()
        ).await
    }

    /// Fetch current game package files update info, from the currently
    /// selected version to the latest available one.
    pub async fn fetch_update_info(
        &self
    ) -> Result<SophonApiPackageUpdateInfo, SophonApiError> {
        self.api.fetch_package_update_info(
            self.region,
            self.branch.clone(),
            self.password.clone(),
            self.package_id.clone(),
            self.version.clone()
        ).await
    }

    /// Try to find download info with given category id, category name or
    /// manifest name, for the current game package.
    pub async fn find_download_manifest(
        &self,
        query: &str
    ) -> Result<Option<package_download_info::SophonApiPackageManifest>, SophonApiError> {
        let manifest = self.fetch_download_info().await?.manifests
            .into_iter()
            .find(|info| {
                info.category_id == query
                    || info.category_name == query
                    || info.name == query
            });

        Ok(manifest)
    }

    /// Try to find patch info with given category id, category name or
    /// manifest name, for the current game package.
    pub async fn find_update_manifest(
        &self,
        query: &str
    ) -> Result<Option<package_update_info::SophonApiPackageManifest>, SophonApiError> {
        let manifest = self.fetch_update_info().await?.manifests
            .into_iter()
            .find(|info| {
                info.category_id == query
                    || info.category_name == query
                    || info.name == query
            });

        Ok(manifest)
    }
}
