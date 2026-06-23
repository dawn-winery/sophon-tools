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

use std::path::PathBuf;
use std::time::Duration;

use md5::{Md5, Digest};
use prost::{Message, DecodeError};

use crate::api::package_download_info::SophonApiPackageManifest;
use crate::protos::{SophonDownloadAssetsInfo, SophonDownloadAssetsInfoAsset};

#[derive(Debug, thiserror::Error)]
pub enum SophonDownloaderError {
    #[error("failed to perform http request: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("failed to perform io operation: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to decode protobuf: {0}")]
    Protobuf(#[from] DecodeError),

    #[error("encrypted files are not supported")]
    EncryptionNotSupported,

    #[error("expected '{expected}' manifest hash, got '{actual}'")]
    ManifestHashMismatch {
        actual: String,
        expected: String
    }
}

pub type AssetsSorter = Box<dyn Fn(
    &SophonDownloadAssetsInfoAsset,
    &SophonDownloadAssetsInfoAsset
) -> std::cmp::Ordering>;

pub struct SophonDownloader {
    client: reqwest::Client,

    fetch_manifest_timeout: Duration,

    verify_manifest_hash: bool,

    disk_cache_directory: PathBuf,

    assets_sorter: Option<AssetsSorter>
}

impl Default for SophonDownloader {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/v{}", crate::VERSION))
                .build()
                .expect("failed to build reqwest client"),

            fetch_manifest_timeout: Duration::from_secs(5),

            verify_manifest_hash: true,

            disk_cache_directory: PathBuf::from(".cache"),

            assets_sorter: None
        }
    }
}

impl SophonDownloader {
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;

        self
    }

    pub fn with_fetch_manifest_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_manifest_timeout = timeout;

        self
    }

    /// Verify downloaded manifests hashes before trying to decode them.
    pub fn with_verify_manifest_hash(mut self, verify_hash: bool) -> Self {
        self.verify_manifest_hash = verify_hash;

        self
    }

    /// Path to the directory in which temporary data is stored.
    pub fn with_disk_cache_directory(mut self, path: PathBuf) -> Self {
        self.disk_cache_directory = path;

        self
    }

    /// Callback used to sort the assets before downloading them. It can be used
    /// if you need to make sure that files will be downloaded in the right
    /// order.
    pub fn with_assets_sorter(mut self, sorter: AssetsSorter) -> Self {
        self.assets_sorter = Some(sorter);

        self
    }

    /// Fetch information about the assets and chunks that the downloader will
    /// need to download.
    pub async fn fetch_download_info(
        &self,
        download_info: &SophonApiPackageManifest
    ) -> Result<SophonDownloadAssetsInfo, SophonDownloaderError> {
        if download_info.manifest_download.encrypted {
            return Err(SophonDownloaderError::EncryptionNotSupported);
        }

        let url = format!(
            "{}{}/{}",
            download_info.manifest_download.url_prefix,
            download_info.manifest_download.url_suffix,
            download_info.manifest_info.id
        );

        let cache_entry = self.disk_cache_directory
            .join("download_manifests")
            .join(format!("{:0x}", seahash::hash(url.as_bytes())));

        if cache_entry.exists() {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?url,
                "read cached sophon download assets manifest"
            );

            let cache_entry = std::fs::read(cache_entry)?;

            return Ok(SophonDownloadAssetsInfo::decode(cache_entry.as_slice())?);
        }

        if let Some(cache_dir) = cache_entry.parent() && !cache_dir.exists() {
            std::fs::create_dir_all(cache_dir)?;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?url,
            "fetch sophon download assets manifest"
        );

        // Download the manifest.
        let response = self.client.get(&url)
            .timeout(self.fetch_manifest_timeout)
            .send()
            .await?;

        let mut manifest = response.bytes().await?.to_vec();

        if download_info.manifest_download.compressed {
            manifest = zstd::decode_all(manifest.as_slice())?;
        }

        // Verify downloaded manifest hash.
        if self.verify_manifest_hash {
            let hash = hex::encode(Md5::digest(&manifest));

            if hash != download_info.manifest_info.hash_md5 {
                return Err(SophonDownloaderError::ManifestHashMismatch {
                    actual: hash,
                    expected: download_info.manifest_info.hash_md5.clone()
                });
            }
        }

        match SophonDownloadAssetsInfo::decode(manifest.as_slice()) {
            Err(err) => Err(SophonDownloaderError::from(err)),

            Ok(decoded_manifest) => {
                // Cache the manifest only if it can be decoded successfully.
                std::fs::write(cache_entry, &manifest)?;

                Ok(decoded_manifest)
            }
        }
    }

    pub async fn download(
        self,
        _download_info: &SophonApiPackageManifest
    ) -> Result<(), SophonDownloaderError> {
        todo!()
    }
}

#[test]
fn test() {
    let runtime = tokio::runtime::Runtime::new()
        .unwrap();

    let api = crate::api::SophonApi::default();

    runtime.block_on(async move {
        let game = api.game(
            crate::region::SophonRegion::Global,
            None,
            String::from("U5hbdsT9W7")
        );

        let package = game.package(None).await.unwrap();

        let manifest = package.find_download_manifest("game").await
            .unwrap()
            .unwrap();

        let manifest = SophonDownloader::default()
            .fetch_download_info(&manifest).await.unwrap();

        dbg!(&manifest.assets[..3]);
    });
}
