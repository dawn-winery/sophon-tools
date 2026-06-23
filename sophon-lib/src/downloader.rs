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

use crate::api::package_download_info::SophonApiPackageManifest;
use crate::protos::SophonDownloadAssetsInfo;

#[derive(Debug, thiserror::Error)]
pub enum SophonDownloaderError {
    #[error("failed to perform http request: {0}")]
    Reqwest(#[from] reqwest::Error)
}

pub struct SophonDownloader {
    client: reqwest::Client
}

impl Default for SophonDownloader {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/v{}", crate::VERSION))
                .build()
                .expect("failed to build reqwest client")
        }
    }
}

impl SophonDownloader {
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;

        self
    }

    pub async fn fetch_download_info(
        &self,
        info: &SophonApiPackageManifest
    ) -> Result<SophonDownloadAssetsInfo, SophonDownloaderError> {
        let url = format!(
            "{}{}/{}",
            info.manifest_download.url_prefix,
            info.manifest_download.url_suffix,
            info.manifest_info.id
        );

        todo!()
    }

    pub async fn download(self) {

    }
}
