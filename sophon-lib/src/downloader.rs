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

use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

use tokio::sync::{Mutex, RwLock};

use md5::{Md5, Digest};
use prost::{Message, DecodeError};

use crate::api::package_download_info::SophonApiPackageManifest;
use crate::protos::{SophonDownloadAssetsInfo, SophonDownloadAssetsInfoAsset};
use crate::verifier::{SophonVerifier, VerifyResult};

#[derive(Debug, thiserror::Error)]
pub enum SophonDownloaderError {
    #[error("failed to perform http request: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("failed to perform io operation: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to open zstd decoder: {0}")]
    Zstd(#[from] ruzstd::decoding::errors::FrameDecoderError),

    #[error("failed to decode protobuf: {0}")]
    Protobuf(#[from] DecodeError),

    #[error("failed to await async task: {0}")]
    Tokio(#[from] tokio::task::JoinError),

    #[error("encrypted files are not supported")]
    EncryptionNotSupported,

    #[error("expected '{expected}' manifest size, got '{actual}'")]
    ManifestSizeMismatch {
        actual: u64,
        expected: u64
    },

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

pub type AssetsFilter = Box<dyn Fn(&SophonDownloadAssetsInfoAsset) -> bool>;

struct CacheSlot {
    pub url: String,
    pub value: SophonDownloadAssetsInfo
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SophonDownloaderVerifyMethod {
    /// Verify both file size and its hash.
    #[default]
    Full,

    /// Verify only file size.
    Fast,

    /// Do not verify files.
    None
}

pub struct SophonDownloader {
    client: reqwest::Client,
    runtime: Option<tokio::runtime::Handle>,

    fetch_manifest_timeout: Option<Duration>,
    fetch_chunk_timeout_per_mb: Option<Duration>,

    verify_manifest: SophonDownloaderVerifyMethod,
    verify_before_downloading: SophonDownloaderVerifyMethod,

    target_memory_usage: u64,

    assets_sorter: Option<AssetsSorter>,
    assets_filter: Option<AssetsFilter>,

    download_manifest_cache: RwLock<Vec<CacheSlot>>
}

impl Default for SophonDownloader {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/v{}", crate::VERSION))
                .build()
                .expect("failed to build reqwest client"),

            runtime: None,

            fetch_manifest_timeout: None,
            fetch_chunk_timeout_per_mb: None,

            verify_manifest: SophonDownloaderVerifyMethod::default(),
            verify_before_downloading: SophonDownloaderVerifyMethod::default(),

            target_memory_usage: 256 * 1024 * 1024,

            assets_sorter: None,
            assets_filter: None,

            download_manifest_cache: RwLock::const_new(Vec::with_capacity(1))
        }
    }
}

impl SophonDownloader {
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;

        self
    }

    /// Tokio runtime handle to use for async operations.
    ///
    /// If unset, then `tokio::spawn` function will be used to schedule tasks.
    pub fn with_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.runtime = Some(runtime);

        self
    }

    /// Manifest downloading timeout.
    ///
    /// Unset by default.
    pub fn with_fetch_manifest_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_manifest_timeout = Some(timeout);

        self
    }

    /// Chunk downloading timeout per MB of data. In other words, for 6.37 MB
    /// chunk downloader will wait for `ceil(6.47) * timeout = 7 * timeout`.
    ///
    /// Unset by default.
    pub fn with_fetch_chunk_timeout_per_mb(mut self, timeout: Duration) -> Self {
        self.fetch_chunk_timeout_per_mb = Some(timeout);

        self
    }

    /// Verify downloaded manifests hashes before trying to decode them.
    pub fn with_verify_manifest_hash(
        mut self,
        method: SophonDownloaderVerifyMethod
    ) -> Self {
        self.verify_manifest = method;

        self
    }

    /// Verify files if they're already available on disk before downloading
    /// them. If disabled, the algorithm will not spend time on verifying files
    /// and will overwrite them instead.
    pub fn with_verify_before_downloading(
        mut self,
        method: SophonDownloaderVerifyMethod
    ) -> Self {
        self.verify_before_downloading = method;

        self
    }

    /// Target memory usage is the amount of system memory downloader will try
    /// to use for downloading files' chunks. The actual usage may be higher,
    /// but should not be lower if there's enough chunks to download.
    ///
    /// Default: `256 MB`
    pub fn with_target_memory_usage(mut self, size: u64) -> Self {
        self.target_memory_usage = size;

        self
    }

    /// Callback used to sort the assets before downloading them. It can be used
    /// if you need to make sure that files will be downloaded in the right
    /// order.
    pub fn with_assets_sorter(mut self, sorter: AssetsSorter) -> Self {
        self.assets_sorter = Some(sorter);

        self
    }

    /// Callback used to filter the assets before downloading them. It can be
    /// used to make downloader ignore some files.
    pub fn with_assets_filter(mut self, filter: AssetsFilter) -> Self {
        self.assets_filter = Some(filter);

        self
    }

    #[inline]
    pub const fn client(&self) -> &reqwest::Client {
        &self.client
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

        if let Some(slot) = self.download_manifest_cache.read().await.iter()
            .find(|slot| slot.url == url)
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?url,
                "read download assets manifest from cache"
            );

            return Ok(slot.value.clone());
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?url,
            "fetch sophon download assets manifest"
        );

        // Download the manifest.
        let response = self.client.get(&url)
            .timeout(self.fetch_manifest_timeout.unwrap_or(Duration::MAX))
            .send()
            .await?;

        let mut manifest = response.bytes().await?.to_vec();

        if download_info.manifest_download.compressed {
            let mut decoder = ruzstd::decoding::StreamingDecoder::new(manifest.as_slice())?;
            let mut buf = Vec::with_capacity(manifest.len());

            decoder.read_to_end(&mut buf)?;

            manifest = buf;
        }

        // Verify downloaded manifest.
        if self.verify_manifest != SophonDownloaderVerifyMethod::None {
            if manifest.len() as u64 != download_info.manifest_info.decompressed_size {
                return Err(SophonDownloaderError::ManifestSizeMismatch {
                    actual: manifest.len() as u64,
                    expected: download_info.manifest_info.decompressed_size
                });
            }

            else if self.verify_manifest == SophonDownloaderVerifyMethod::Full {
                let hash = hex::encode(Md5::digest(&manifest));

                if hash != download_info.manifest_info.hash_md5 {
                    return Err(SophonDownloaderError::ManifestHashMismatch {
                        actual: hash,
                        expected: download_info.manifest_info.hash_md5.clone()
                    });
                }
            }
        }

        match SophonDownloadAssetsInfo::decode(manifest.as_slice()) {
            Err(err) => Err(SophonDownloaderError::from(err)),

            Ok(decoded_manifest) => {
                // Cache the manifest only if it can be decoded successfully.
                self.download_manifest_cache.write().await.push(CacheSlot {
                    url,
                    value: decoded_manifest.clone()
                });

                Ok(decoded_manifest)
            }
        }
    }

    /// Download files (assets) to the given directory.
    ///
    /// The downloader will skip already downloaded files.
    ///
    /// ## Downloader strategy
    ///
    /// 1. Prepare list of assets with applied filter function provided by user.
    /// 2. Sort every asset by their total chunks decompressed size in ascending
    ///    order (so smaller assets are placed first).
    /// 3. If user provided a sort function, then apply it to the assets list.
    ///
    /// Then, the user provides us with the `target_memory_usage` property. It
    /// should indicate how much memory *in average* we want to spend on
    /// assembling assets.
    ///
    /// Since we know each asset's decompressed chunks size - we can try to fit
    /// as many full assets assembling to the async runtime as possible, until
    /// we reach the `target_memory_usage` memory usage level.
    ///
    /// 4. Start iterating over the assets list.
    /// 5. While current asset with all its chunks' decompressed size can fit
    ///    inside of the async runtime - create async tasks to download the
    ///    chunks and write their content to a shared buffered file mutex.
    ///
    /// Once we fill the runtime with assets assembling tasks up to the
    /// `target_memory_usage` level - next assets won't fit precisely to the
    /// given target level. In that case, we will wait until enough space in
    /// the runtime frees up.
    ///
    /// 6. While there are tasks in the runtime and not enough space to fit
    ///    a new one - iterate over the tasks and wait until they finish.
    ///    When enough space for a new asset appears - return to step 4.
    ///
    /// If user set the `target_memory_usage` level too low and some assets
    /// won't fit into it at all (which will happen more frequently at the end
    /// of the download procedure since the ordering of the assets list) - then
    /// we will wait until all the tasks finish.
    ///
    /// 7. If after waiting until all the tasks finish there's still not enough
    ///    space to fit the asset - create new task for it and only it anyway.
    pub async fn download(
        self,
        download_info: &SophonApiPackageManifest,
        download_dir: &Path
    ) -> Result<(), SophonDownloaderError> {
        if download_info.chunk_download.encrypted {
            return Err(SophonDownloaderError::EncryptionNotSupported);
        }

        // Fetch list of assets to download.
        let mut download_manifest = self.fetch_download_info(download_info).await?;

        // Clear the cache since it won't be used anymore.
        self.download_manifest_cache.write().await.clear();

        // Skip assets downloading that are valid.
        if self.verify_before_downloading != SophonDownloaderVerifyMethod::None {
            let mut verifier = SophonVerifier::new(download_manifest.assets.clone());

            if self.verify_before_downloading == SophonDownloaderVerifyMethod::Fast {
                verifier = verifier.with_fast_verify(true);
            }

            download_manifest.assets.retain(move |asset| {
                !matches!(
                    verifier.verify_file(download_dir.join(&asset.path)),
                    Ok(VerifyResult::Valid)
                )
            });
        }

        // Apply filter function to the list.
        let mut assets = download_manifest.assets.into_iter()
            .filter(|asset| {
                self.assets_filter.as_ref()
                    .map(|filter| filter(asset))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        // Sort assets by their total chunks size in ascending order, so the
        // first assets will have smallest total decompressed size.
        assets.sort_by(|a, b| {
            let a_size = a.chunks.iter()
                .map(|chunk| chunk.decompressed_size)
                .sum::<u64>();

            let b_size = b.chunks.iter()
                .map(|chunk| chunk.decompressed_size)
                .sum::<u64>();

            a_size.cmp(&b_size)
        });

        // If assets sorter function is provided, then apply it as well.
        if let Some(sorter) = self.assets_sorter {
            assets.sort_by(sorter);
        }

        // Pre-calculate tasks queue capacity.
        let median_task_size = assets.get(assets.len() / 3)
            .map(|asset| {
                asset.chunks.iter()
                    .map(|chunk| chunk.decompressed_size)
                    .sum::<u64>()
            })
            .unwrap_or(u64::MAX);

        let mut tasks = Vec::with_capacity(
            (self.target_memory_usage / median_task_size) as usize
        );

        let mut occupied_memory = 0;

        async fn flatten(
            task: tokio::task::JoinHandle<Result<(), SophonDownloaderError>>
        ) -> Result<(), SophonDownloaderError> {
            match task.await {
                Ok(result) => result,
                Err(err) => Err(SophonDownloaderError::Tokio(err))
            }
        }

        // Iterate over all the assets we need to download.
        while let Some(asset) = assets.pop() {
            // Create file's parent folder if it doesn't exist.
            let asset_path = download_dir.join(&asset.path);

            if let Some(parent) = asset_path.parent() && !parent.is_dir() {
                std::fs::create_dir_all(parent)?;
            }

            // Create new file or open existing one. Pre-allocate space for
            // the file chunks on disk.
            let file = File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(asset_path)?;

            file.set_len(asset.size)?;

            let file = Arc::new(Mutex::new(BufWriter::new(file)));

            // Calculate memory needed to store all file chunks.
            let download_size = asset.chunks.iter()
                .map(|chunk| chunk.decompressed_size)
                .sum::<u64>();

            // If we cannot fit all the file chunks in memory yet AND the tasks
            // queue is not empty - then we wait until already scheduled files
            // finish writing.
            if occupied_memory + download_size > self.target_memory_usage
                && !tasks.is_empty()
            {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    tasks = tasks.len(),
                    ?occupied_memory,
                    "wait for scheduled download tasks"
                );

                futures::future::try_join_all(tasks.drain(..).map(flatten)).await?;

                occupied_memory = 0;
            }

            // Schedule all the chunks of the file.
            for chunk in asset.chunks {
                let file = file.clone();

                let url = format!(
                    "{}{}/{}",
                    download_info.chunk_download.url_prefix,
                    download_info.chunk_download.url_suffix,
                    chunk.name
                );

                #[cfg(feature = "tracing")]
                tracing::trace!(
                    asset = ?asset.path,
                    offset = ?chunk.offset,
                    size = ?chunk.decompressed_size,
                    ?url,
                    "schedule chunk download"
                );

                let decompress_chunk = download_info.chunk_download.compressed;

                let mut request = self.client.get(&url);

                if let Some(timeout_per_mb) = self.fetch_chunk_timeout_per_mb {
                    request = request.timeout({
                        (chunk.compressed_size as f64 / 1024.0 / 1024.0).ceil() as u32
                            * timeout_per_mb
                    });
                }

                occupied_memory += chunk.decompressed_size;

                // Start chunk downloading task.
                let future = async move {
                    let response = request.send().await?;

                    let mut chunk_body = response.bytes().await?.to_vec();

                    if decompress_chunk {
                        let mut decoder = ruzstd::decoding::StreamingDecoder::new(chunk_body.as_slice())?;
                        let mut buf = Vec::with_capacity(chunk_body.len());

                        decoder.read_to_end(&mut buf)?;

                        chunk_body = buf;
                    }

                    let mut lock = file.lock().await;

                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        offset = ?chunk.offset,
                        size = ?chunk.decompressed_size,
                        ?url,
                        "write chunk to disk"
                    );

                    lock.seek(SeekFrom::Start(chunk.offset))?;
                    lock.write_all(&chunk_body)?;
                    lock.flush()?;

                    drop(lock);

                    Ok::<_, SophonDownloaderError>(())
                };

                let task = match &self.runtime {
                    Some(runtime) => runtime.spawn(future),
                    None => tokio::spawn(future)
                };

                tasks.push(task);
            }

            drop(file);
        }

        // Wait for all the remaining tasks to finish.
        futures::future::try_join_all(tasks.into_iter().map(flatten)).await?;

        Ok(())
    }
}
