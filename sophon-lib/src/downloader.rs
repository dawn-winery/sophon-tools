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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::io::{Cursor, Read, SeekFrom};

use tokio::sync::{Mutex, RwLock};
use tokio::io::{BufWriter, AsyncSeekExt, AsyncWriteExt};
use tokio::fs::File;

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

    #[error("expected '{expected}' manifest size, got '{actual}', url: '{url}'")]
    ManifestSizeMismatch {
        url: String,
        actual: u64,
        expected: u64
    },

    #[error("expected '{expected}' manifest hash, got '{actual}', url: '{url}'")]
    ManifestHashMismatch {
        url: String,
        actual: String,
        expected: String
    },

    #[error("expected '{expected}' chunk size, got '{actual}', url: '{url}'")]
    ChunkSizeMismatch {
        url: String,
        actual: u64,
        expected: u64
    },

    #[error("expected '{expected}' chunk hash, got '{actual}', url: '{url}'")]
    ChunkHashMismatch {
        url: String,
        actual: String,
        expected: String
    }
}

pub type AssetsFilter = Box<dyn Fn(&SophonDownloadAssetsInfoAsset) -> bool>;

pub type AssetsSorter = Box<dyn Fn(
    &SophonDownloadAssetsInfoAsset,
    &SophonDownloadAssetsInfoAsset
) -> std::cmp::Ordering>;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SophonDownloaderProgressMsg {
    /// Verify game assets before downloading.
    Verify {
        current: u64,
        total: u64,
        path: PathBuf,
        result: VerifyResult
    },

    /// Download assets chunks.
    Download {
        current: u64,
        total: u64
    }
}

pub struct SophonDownloader {
    client: reqwest::Client,
    runtime: Option<tokio::runtime::Handle>,

    fetch_manifest_timeout: Option<Duration>,
    fetch_chunk_timeout_per_mb: Option<Duration>,

    verify_manifest: SophonDownloaderVerifyMethod,
    verify_chunks: SophonDownloaderVerifyMethod,
    verify_before_downloading: SophonDownloaderVerifyMethod,

    target_memory_usage: u64,
    chunk_download_attempts: u8,

    assets_filter: Option<AssetsFilter>,
    assets_sorter: Option<AssetsSorter>,

    manifest_cache: RwLock<Vec<CacheSlot>>
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

            verify_manifest: SophonDownloaderVerifyMethod::Full,
            verify_chunks: SophonDownloaderVerifyMethod::Fast,
            verify_before_downloading: SophonDownloaderVerifyMethod::Full,

            target_memory_usage: 256 * 1024 * 1024,
            chunk_download_attempts: 3,

            assets_filter: None,
            assets_sorter: None,

            manifest_cache: RwLock::const_new(Vec::with_capacity(1))
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
    /// chunk downloader will wait for `ceil(6.37) * timeout = 7 * timeout`.
    ///
    /// Unset by default.
    pub fn with_fetch_chunk_timeout_per_mb(mut self, timeout: Duration) -> Self {
        self.fetch_chunk_timeout_per_mb = Some(timeout);

        self
    }

    /// Verify downloaded manifest before trying to decode it.
    ///
    /// Default: `Full` (size + hash)
    pub fn with_verify_manifest(
        mut self,
        method: SophonDownloaderVerifyMethod
    ) -> Self {
        self.verify_manifest = method;

        self
    }

    /// Verify downloaded chunks before trying to apply them. If disabled,
    /// chunks will be written to disk without any verification.
    ///
    /// Default: `Fast` (only chunk size)
    pub fn with_verify_chunks(
        mut self,
        method: SophonDownloaderVerifyMethod
    ) -> Self {
        self.verify_chunks = method;

        self
    }

    /// Verify files if they're already available on disk before downloading
    /// them. If disabled, the algorithm will not spend time on finding files
    /// that need to be downloaded, and will download every file instead.
    ///
    /// Default: `Full` (size + hash)
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


    /// Amount of time downloader will try to download a chunk.
    ///
    /// Sometimes remote server drops the connection, so we can try to
    /// download the same chunk multiple times.
    ///
    /// Default: `3`
    pub fn with_chunk_download_attempts(mut self, attempts: u8) -> Self {
        self.chunk_download_attempts = attempts;

        self
    }

    /// Callback used to filter the assets before downloading them. It can be
    /// used to make downloader ignore some files.
    pub fn with_assets_filter(mut self, filter: AssetsFilter) -> Self {
        self.assets_filter = Some(filter);

        self
    }

    /// Callback used to sort the assets before downloading them. It can be used
    /// if you need to make sure that files will be downloaded in the right
    /// order.
    pub fn with_assets_sorter(mut self, sorter: AssetsSorter) -> Self {
        self.assets_sorter = Some(sorter);

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

        if let Some(slot) = self.manifest_cache.read().await.iter()
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

        // Verify response size.
        if self.verify_manifest != SophonDownloaderVerifyMethod::None
            && let Some(content_length) = response.content_length()
            && content_length != download_info.manifest_info.compressed_size
        {
            return Err(SophonDownloaderError::ChunkSizeMismatch {
                url,
                actual: content_length,
                expected: download_info.manifest_info.compressed_size
            });
        }

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
                    url,
                    actual: manifest.len() as u64,
                    expected: download_info.manifest_info.decompressed_size
                });
            }

            else if self.verify_manifest == SophonDownloaderVerifyMethod::Full {
                let hash = hex::encode(Md5::digest(&manifest));

                if hash != download_info.manifest_info.hash_md5 {
                    return Err(SophonDownloaderError::ManifestHashMismatch {
                        url,
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
                self.manifest_cache.write().await.push(CacheSlot {
                    url,
                    value: decoded_manifest.clone()
                });

                Ok(decoded_manifest)
            }
        }
    }

    /// Download files (assets) to the given directory.
    ///
    /// ## Downloader strategy
    ///
    /// 1. Prepare list of assets with applied filter function provided by user.
    /// 2. Sort every asset by their total chunks decompressed size in ascending
    ///    order (so smaller assets are placed first).
    /// 3. If user provided a sort function, then apply it to the assets list.
    ///
    /// Then, the user provides us with the `target_memory_usage` property. It
    /// should indicate how much memory *on average* we want to spend on
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
        download_dir: &Path,
        progress_updater: Box<dyn Fn(SophonDownloaderProgressMsg) + Send + Sync>
    ) -> Result<(), SophonDownloaderError> {
        if download_info.chunk_download.encrypted {
            return Err(SophonDownloaderError::EncryptionNotSupported);
        }

        // Fetch list of assets to download.
        let mut download_manifest = self.fetch_download_info(download_info).await?;

        // Clear the cache since it won't be used anymore.
        self.manifest_cache.write().await.clear();

        let progress_updater = Arc::new(progress_updater);

        // Skip assets downloading that are valid.
        if self.verify_before_downloading != SophonDownloaderVerifyMethod::None {
            let mut verifier = SophonVerifier::from(
                download_manifest.assets.clone()
            );

            if let Some(runtime) = self.runtime.clone() {
                verifier = verifier.with_runtime(runtime);
            }

            if self.verify_before_downloading == SophonDownloaderVerifyMethod::Fast {
                verifier = verifier.with_fast_verify(true);
            }

            let progress_updater = progress_updater.clone();

            // Pre-verify all the directory files in parallel.
            verifier.scan_directory(
                download_dir.to_path_buf(),
                Box::new(move |update| {
                    progress_updater(SophonDownloaderProgressMsg::Verify {
                        current: update.current,
                        total: update.total,
                        path: update.path,
                        result: update.result
                    });
                })
            ).await?;

            let mut valid_assets = HashSet::with_capacity(
                download_manifest.assets.len()
            );

            for asset in &download_manifest.assets {
                if matches!(
                    verifier.verify_file(download_dir.join(&asset.path)).await,
                    Ok(VerifyResult::Valid)
                ) {
                    valid_assets.insert(asset.path.clone());
                }
            }

            download_manifest.assets.retain(|asset| {
                !valid_assets.contains(&asset.path)
            });
        }

        // Apply filter function to the list.
        let assets = download_manifest.assets.into_iter()
            .filter(|asset| {
                self.assets_filter.as_ref()
                    .map(|filter| filter(asset))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        // Sort assets by their total chunks size in descending order, so the
        // first assets will have largest total decompressed size.
        let mut assets = assets.into_iter()
            .map(|asset| {
                let size = asset.chunks.iter()
                    .map(|chunk| chunk.decompressed_size)
                    .sum::<u64>();

                (asset, size)
            })
            .collect::<Vec<_>>();

        #[allow(clippy::unnecessary_sort_by)]
        assets.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // If assets sorter function is provided, then apply it as well.
        if let Some(sorter) = self.assets_sorter {
            assets.sort_by(|a, b| sorter(&a.0, &b.0));
        }

        // Pre-calculate tasks queue capacity.
        let median_task_size = assets.get(assets.len() / 3)
            .map(|(_, size)| *size)
            .unwrap_or(u64::MAX);

        let mut tasks = Vec::with_capacity(
            (self.target_memory_usage / median_task_size).max(1) as usize
        );

        let mut occupied_memory = 0;

        // Calculate total download assets for progress reporting.
        let progress_current = Arc::new(AtomicU64::new(0));

        let progress_total = assets.iter()
            .map(|(_, size)| *size)
            .sum::<u64>();

        async fn flatten(
            task: tokio::task::JoinHandle<Result<(), SophonDownloaderError>>
        ) -> Result<(), SophonDownloaderError> {
            match task.await {
                Ok(result) => result,
                Err(err) => Err(SophonDownloaderError::Tokio(err))
            }
        }

        // Iterate over all the assets we need to download.
        while let Some((asset, _)) = assets.pop() {
            // Create file's parent folder if it doesn't exist.
            let asset_path = download_dir.join(&asset.path);

            if let Some(parent) = asset_path.parent() && !parent.is_dir() {
                tokio::fs::create_dir_all(parent).await?;
            }

            // Create new file or open existing one. Pre-allocate space for
            // the file chunks on disk.
            let file = File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(asset_path)
                .await?;

            file.set_len(asset.size).await?;

            let file = Arc::new(Mutex::new(
                // Increase default buffer to 64 KB because chunks are large.
                BufWriter::with_capacity(64 * 1024, file)
            ));

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

                futures::future::try_join_all(
                    tasks.drain(..)
                        .map(flatten)
                ).await?;

                occupied_memory = 0;
            }

            // Schedule all the chunks of the file.
            for chunk in asset.chunks {
                let file = file.clone();

                let progress_current = progress_current.clone();
                let progress_updater = progress_updater.clone();

                let chunk_download_url = format!(
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
                    url = ?chunk_download_url,
                    "schedule chunk download"
                );

                let decompress_chunk = download_info.chunk_download.compressed;

                let mut request = self.client.get(&chunk_download_url);

                if let Some(timeout_per_mb) = self.fetch_chunk_timeout_per_mb {
                    request = request.timeout({
                        (chunk.compressed_size as f64 / 1024.0 / 1024.0).ceil() as u32
                            * timeout_per_mb
                    });
                }

                occupied_memory += chunk.decompressed_size;

                // Start chunk downloading task.
                let future = async move {
                    let request_copy = request.try_clone();

                    let response = request.send().await?;

                    // Verify response size.
                    if self.verify_chunks != SophonDownloaderVerifyMethod::None
                        && let Some(content_length) = response.content_length()
                        && content_length != chunk.compressed_size
                    {
                        return Err(SophonDownloaderError::ChunkSizeMismatch {
                            url: chunk_download_url,
                            actual: content_length,
                            expected: chunk.compressed_size
                        });
                    }

                    let mut chunk_body = response.bytes().await;

                    if let Some(request) = request_copy && chunk_body.is_err() {
                        for attempt in 1..self.chunk_download_attempts {
                            // Wait for some time before making new download
                            // attempt: 100ms, 200ms, 400ms, 800ms, 1600ms, ...
                            tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;

                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                url = ?chunk_download_url,
                                ?attempt,
                                "failed to download chunk"
                            );

                            let Some(request) = request.try_clone() else {
                                break;
                            };

                            let Ok(request) = request.send().await else {
                                continue;
                            };

                            chunk_body = request.bytes().await;

                            if chunk_body.is_ok() {
                                break;
                            }
                        }
                    }

                    let mut chunk_body = chunk_body?.to_vec();

                    if decompress_chunk {
                        let mut decoder = ruzstd::decoding::StreamingDecoder::new(
                            Cursor::new(chunk_body)
                        )?;

                        let mut buf = Vec::with_capacity(
                            chunk.decompressed_size as usize
                        );

                        decoder.read_to_end(&mut buf)?;

                        chunk_body = buf;
                    }

                    // Verify downloaded chunk.
                    if self.verify_chunks != SophonDownloaderVerifyMethod::None {
                        if chunk_body.len() as u64 != chunk.decompressed_size {
                            return Err(SophonDownloaderError::ChunkSizeMismatch {
                                url: chunk_download_url,
                                actual: chunk_body.len() as u64,
                                expected: chunk.decompressed_size
                            });
                        }

                        else if self.verify_chunks == SophonDownloaderVerifyMethod::Full {
                            let hash = hex::encode(Md5::digest(&chunk_body));

                            if hash != chunk.decompressed_hash_md5 {
                                return Err(SophonDownloaderError::ChunkHashMismatch {
                                    url: chunk_download_url,
                                    actual: hash,
                                    expected: chunk.decompressed_hash_md5
                                });
                            }
                        }
                    }

                    let mut lock = file.lock().await;

                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        offset = ?chunk.offset,
                        size = ?chunk.decompressed_size,
                        url = ?chunk_download_url,
                        "write chunk to disk"
                    );

                    lock.seek(SeekFrom::Start(chunk.offset)).await?;
                    lock.write_all(&chunk_body).await?;
                    lock.flush().await?;

                    drop(lock);

                    let current = progress_current.fetch_add(
                        chunk.decompressed_size,
                        Ordering::Relaxed
                    );

                    progress_updater(SophonDownloaderProgressMsg::Download {
                        current: current + chunk.decompressed_size,
                        total: progress_total
                    });

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
        futures::future::try_join_all(
            tasks.into_iter()
                .map(flatten)
        ).await?;

        Ok(())
    }
}
