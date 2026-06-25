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

use std::path::{Path, PathBuf};
use std::time::Duration;
use std::io::Read;

use tokio::sync::RwLock;

use md5::{Md5, Digest};
use prost::{Message, DecodeError};

use crate::api::package_update_info::SophonApiPackageManifest;
use crate::protos::{SophonUpdateAssetsInfo, SophonUpdateAssetsInfoAsset};
use crate::verifier::{SophonVerifier, VerifyResult};

#[derive(Debug, thiserror::Error)]
pub enum SophonUpdaterError {
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

    #[error("expected '{expected}' chunk size, got '{actual}', url: '{url}' (offset: '{offset}', length: '{length}')")]
    ChunkSizeMismatch {
        url: String,
        actual: u64,
        expected: u64,
        offset: u64,
        length: u64
    },

    #[error("expected '{expected}' chunk hash, got '{actual}', url: '{url}' (offset: '{offset}', length: '{length}')")]
    ChunkHashMismatch {
        url: String,
        actual: String,
        expected: String,
        offset: u64,
        length: u64
    }
}

pub type AssetsSorter = Box<dyn Fn(
    &SophonUpdateAssetsInfoAsset,
    &SophonUpdateAssetsInfoAsset
) -> std::cmp::Ordering>;

pub type AssetsFilter = Box<dyn Fn(&SophonUpdateAssetsInfoAsset) -> bool>;

struct CacheSlot {
    pub url: String,
    pub value: SophonUpdateAssetsInfo
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SophonUpdaterVerifyMethod {
    /// Verify both file size and its hash.
    #[default]
    Full,

    /// Verify only file size.
    Fast,

    /// Do not verify files.
    None
}

pub struct SophonUpdater {
    client: reqwest::Client,
    runtime: Option<tokio::runtime::Handle>,

    fetch_manifest_timeout: Option<Duration>,
    fetch_chunk_timeout_per_mb: Option<Duration>,

    verify_manifest: SophonUpdaterVerifyMethod,
    verify_chunks: SophonUpdaterVerifyMethod,
    verify_before_updating: SophonUpdaterVerifyMethod,

    delete_unused_files: bool,
    patch_files: bool,
    delete_chunks: bool,

    target_memory_usage: u64,
    chunk_download_attempts: u8,

    assets_sorter: Option<AssetsSorter>,
    assets_filter: Option<AssetsFilter>,

    manifest_cache: RwLock<Vec<CacheSlot>>
}

impl Default for SophonUpdater {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(format!("sophon-tools/v{}", crate::VERSION))
                .build()
                .expect("failed to build reqwest client"),

            runtime: None,

            fetch_manifest_timeout: None,
            fetch_chunk_timeout_per_mb: None,

            verify_manifest: SophonUpdaterVerifyMethod::Full,
            verify_chunks: SophonUpdaterVerifyMethod::Fast,
            verify_before_updating: SophonUpdaterVerifyMethod::Full,

            delete_unused_files: true,
            patch_files: true,
            delete_chunks: true,

            target_memory_usage: 256 * 1024 * 1024,
            chunk_download_attempts: 3,

            assets_sorter: None,
            assets_filter: None,

            manifest_cache: RwLock::const_new(Vec::with_capacity(1))
        }
    }
}

impl SophonUpdater {
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
    pub fn with_verify_manifest(
        mut self,
        method: SophonUpdaterVerifyMethod
    ) -> Self {
        self.verify_manifest = method;

        self
    }

    /// Verify downloaded chunks before trying to apply them. If disabled,
    /// chunks will be applied without any verification.
    ///
    /// Default: `Fast` (only chunk size)
    pub fn with_verify_chunks(
        mut self,
        method: SophonUpdaterVerifyMethod
    ) -> Self {
        self.verify_chunks = method;

        self
    }

    /// Verify files if they're already available on disk before updating
    /// them. If disabled, the algorithm will not spend time on verifying files
    /// and will overwrite them instead.
    ///
    /// Default: `Full` (size + hash)
    pub fn with_verify_before_updating(
        mut self,
        method: SophonUpdaterVerifyMethod
    ) -> Self {
        self.verify_before_updating = method;

        self
    }

    /// Delete game files that were marked as unused.
    ///
    /// Default: `true`
    pub fn with_delete_unused_files(mut self, delete_unused: bool) -> Self {
        self.delete_unused_files = delete_unused;

        self
    }

    /// Apply downloaded chunks to the game files. If disabled, the chunks will
    /// be stored on disk and could be reused on the next updater execution.
    ///
    /// Default: `true`
    pub fn with_patch_files(mut self, patch_files: bool) -> Self {
        self.patch_files = patch_files;

        self
    }

    /// Delete downloaded chunks when they become useless.
    ///
    /// If game files patching is enabled, then related chunks will be deleted
    /// when the game file is patched.
    ///
    /// Default: `true`
    pub fn with_delete_chunks(mut self, delete_chunks: bool) -> Self {
        self.delete_chunks = delete_chunks;

        self
    }

    /// Target memory usage is the amount of system memory updater will try
    /// to use for downloading files' chunks. The actual usage may be higher,
    /// but should not be lower if there's enough chunks to download.
    ///
    /// Default: `256 MB`
    pub fn with_target_memory_usage(mut self, size: u64) -> Self {
        self.target_memory_usage = size;

        self
    }

    /// Amount of time updater will try to download a chunk.
    ///
    /// Sometimes remote server drops the connection, so we can try to
    /// download the same chunk multiple times.
    ///
    /// Default: `3`
    pub fn with_chunk_download_attempts(mut self, attempts: u8) -> Self {
        self.chunk_download_attempts = attempts;

        self
    }

    /// Callback used to sort the assets before updating them. It can be used
    /// if you need to make sure that files will be updated in the right
    /// order.
    pub fn with_assets_sorter(mut self, sorter: AssetsSorter) -> Self {
        self.assets_sorter = Some(sorter);

        self
    }

    /// Callback used to filter the assets before updating them. It can be
    /// used to make updater ignore some files.
    pub fn with_assets_filter(mut self, filter: AssetsFilter) -> Self {
        self.assets_filter = Some(filter);

        self
    }

    #[inline]
    pub const fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Fetch information about the assets and chunks that the updater will
    /// need to update.
    pub async fn fetch_manifest(
        &self,
        download_info: &SophonApiPackageManifest
    ) -> Result<SophonUpdateAssetsInfo, SophonUpdaterError> {
        if download_info.manifest_download.encrypted {
            return Err(SophonUpdaterError::EncryptionNotSupported);
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
                "read update assets manifest from cache"
            );

            return Ok(slot.value.clone());
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?url,
            "fetch sophon update assets manifest"
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
        if self.verify_manifest != SophonUpdaterVerifyMethod::None {
            if manifest.len() as u64 != download_info.manifest_info.decompressed_size {
                return Err(SophonUpdaterError::ManifestSizeMismatch {
                    url,
                    actual: manifest.len() as u64,
                    expected: download_info.manifest_info.decompressed_size
                });
            }

            else if self.verify_manifest == SophonUpdaterVerifyMethod::Full {
                let hash = hex::encode(Md5::digest(&manifest));

                if hash != download_info.manifest_info.hash_md5 {
                    return Err(SophonUpdaterError::ManifestHashMismatch {
                        url,
                        actual: hash,
                        expected: download_info.manifest_info.hash_md5.clone()
                    });
                }
            }
        }

        match SophonUpdateAssetsInfo::decode(manifest.as_slice()) {
            Err(err) => Err(SophonUpdaterError::from(err)),

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

    /// Update gane files (assets) stored in the `update_dir` using chunks
    /// stored in the `chunks_dir`, or by downloading chunks from server.
    /// `update_version` indicates the version of the game in the `update_dir`
    /// directory.
    ///
    /// Some files cannot be updated from one version to another. You will
    /// likely need to run a separate downloader tasks after the update to
    /// re-download these files.
    ///
    /// ## Updater strategy
    ///
    /// Updater works in 3 main stages:
    ///
    /// - Removing unused assets to free up disk space;
    /// - Downloading assets' chunks (patches);
    /// - Applying patches to the game assets.
    ///
    /// The chunks downloading and applying stages were intentionally separated
    /// for different reasons. This can change in future.
    ///
    /// 1. Prepare list of assets with applied filter function provided by user.
    /// 2. Sort every asset by their total chunks size in ascending order
    ///    (so smaller assets are placed first).
    /// 3. If user provided a sort function, then apply it to the assets list.
    ///
    /// Before downloading new data, the update info provides us with the list
    /// of files that are not used anymore, so we will delete them to free up
    /// the disk space.
    ///
    /// 4. Start iterating over the unused assets list.
    /// 5. If the asset is available in the `update_dir` - then delete it.
    ///
    /// Then, the user provides us with the `target_memory_usage` property. It
    /// should indicate how much memory *on average* we want to spend on
    /// downloading assets' chunks and patching game files.
    ///
    /// Since we know each asset's chunk size - we can try to fit as many full
    /// assets' chunks downloading to the async runtime as possible, until we
    /// reach the `target_memory_usage` memory usage level.
    ///
    /// 6. Start iterating over the assets list.
    /// 7. Look at the download directory for the asset's chunk. If there is,
    ///    then skip asset's chunk downloading step.
    /// 8. If current asset with its chunk's size can fit inside of the async
    ///    runtime - create async task to download the chunk and write it to the
    ///    chunks download directory.
    ///
    /// Once we fill the runtime with assets' chunks downloading tasks up to the
    /// `target_memory_usage` level - next assets won't fit precisely to the
    /// given target level. In that case, we will wait until enough space in
    /// the runtime frees up.
    ///
    /// 9. While there are tasks in the runtime and not enough space to fit
    ///    a new one - iterate over the tasks and wait until they finish.
    ///    When enough space for a new asset appears - return to step 6.
    ///
    /// If user set the `target_memory_usage` level too low and some assets
    /// won't fit into it at all (which will happen more frequently at the end
    /// of the chunks downloading procedure since the ordering of the assets
    /// list) - then we will wait until all the tasks finish.
    ///
    /// 10. If after waiting until all the tasks finish there's still not enough
    ///     space to fit the asset - create new task for it and only it anyway.
    ///
    /// When we finish downloading all the chunks - we can start patching game
    /// files.
    ///
    /// 11. Start iterating over the assets' chunks (patches).
    /// 12. If current patch can fit inside of the async runtime - create
    ///     async task to apply the patch.
    /// 13. While there are tasks in the runtime and not enough space to fit
    ///     a new one - iterate over the tasks and wait until they finish.
    ///     When enough space for a new patch appears - return to step 11.
    /// 14. If after waiting until all the tasks finish there's still not enough
    ///     space to fit the patch - create new task for it and only it anyway.
    pub async fn update(
        self,
        update_info: &SophonApiPackageManifest,
        update_version: &str,
        chunks_dir: &Path,
        update_dir: &Path
    ) -> Result<(), SophonUpdaterError> {
        if update_info.diff_download.encrypted {
            return Err(SophonUpdaterError::EncryptionNotSupported);
        }

        // Fetch list of assets to update.
        let mut update_manifest = self.fetch_manifest(update_info).await?;

        // Clear the cache since it won't be used anymore.
        self.manifest_cache.write().await.clear();

        // Skip assets downloading that are valid.
        if self.verify_before_updating != SophonUpdaterVerifyMethod::None {
            let mut verifier = SophonVerifier::from(
                update_manifest.assets.clone()
            );

            if self.verify_before_updating == SophonUpdaterVerifyMethod::Fast {
                verifier = verifier.with_fast_verify(true);
            }

            update_manifest.assets.retain(move |asset| {
                !matches!(
                    verifier.verify_file(chunks_dir.join(&asset.path)),
                    Ok(VerifyResult::Valid)
                )
            });
        }

        // Apply filter function to the list.
        let mut assets = update_manifest.assets.into_iter()
            .filter(|asset| {
                self.assets_filter.as_ref()
                    .map(|filter| filter(asset))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        // Sort assets by their total chunks size in ascending order, so the
        // first assets will have smallest total decompressed size.
        assets.sort_by(|a, b| {
            let a_size = a.chunks.get(update_version)
                .map(|chunk| chunk.chunk_length);

            let b_size = b.chunks.get(update_version)
                .map(|chunk| chunk.chunk_length);

            a_size.cmp(&b_size)
        });

        // If assets sorter function is provided, then apply it as well.
        if let Some(sorter) = self.assets_sorter {
            assets.sort_by(sorter);
        }

        async fn flatten<T>(
            task: tokio::task::JoinHandle<Result<T, SophonUpdaterError>>
        ) -> Result<T, SophonUpdaterError> {
            match task.await {
                Ok(result) => result,
                Err(err) => Err(SophonUpdaterError::Tokio(err))
            }
        }

        // If update has unused assets then delete them.
        if let Some(unused_assets) = update_manifest.unused_assets.remove(update_version)
            && self.delete_unused_files
        {
            // TODO: delete empty folders
            let tasks = unused_assets.files.into_iter()
                .map(|asset| update_dir.join(asset.name))
                .filter(|path| path.exists())
                .map(|path| {
                    let future = async move {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(?path, "delete unused asset");

                        tokio::fs::remove_file(path).await
                            .map_err(SophonUpdaterError::Io)
                    };

                    if let Some(runtime) = &self.runtime {
                        runtime.spawn(future)
                    } else {
                        tokio::spawn(future)
                    }
                })
                .map(flatten);

            futures::future::try_join_all(tasks).await?;
        }

        // Clear the rest of unused assets.
        update_manifest.unused_assets.clear();

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct PatchInfo {
            pub patch_path: PathBuf,
            pub patch_size: u64,

            pub asset_path: PathBuf,

            pub input_asset_size: u64,
            pub input_asset_hash_md5: String,

            pub output_asset_size: u64,
            pub output_asset_hash_md5: String
        }

        // Pre-calculate tasks queue capacity.
        let median_task_size = assets.get(assets.len() / 3)
            .and_then(|asset| asset.chunks.get(update_version))
            .map(|chunk| chunk.chunk_length)
            .unwrap_or(u64::MAX);

        let mut tasks = Vec::with_capacity(
            (self.target_memory_usage / median_task_size).max(1) as usize
        );

        let mut patches = Vec::with_capacity(assets.len());

        let mut occupied_memory = 0;

        // Create chunks download dir if it doesn't exist.
        if !chunks_dir.is_dir() {
            tokio::fs::create_dir_all(chunks_dir).await?;
        }

        // Iterate over all the assets we need to update.
        while let Some(mut asset) = assets.pop() {
            // Skip assets that cannot be updated from the given version. These
            // should be handled separately using downloader as files repairer.
            let Some(chunk) = asset.chunks.remove(update_version) else {
                continue;
            };

            let chunk_download_url = format!(
                "{}{}/{}",
                update_info.diff_download.url_prefix,
                update_info.diff_download.url_suffix,
                chunk.name
            );

            let chunk_download_offset = chunk.chunk_offset;

            let patch_info = PatchInfo {
                patch_path: chunks_dir.join(format!(
                    "{}-{}-{}",
                    chunk.name,
                    chunk.chunk_offset,
                    chunk.chunk_length
                )),

                patch_size: chunk.chunk_length,

                asset_path: update_dir.join(&asset.path),

                input_asset_size: chunk.asset_size,
                input_asset_hash_md5: chunk.asset_hash_md5.clone(),

                output_asset_size: asset.size,
                output_asset_hash_md5: asset.hash_md5.clone()
            };

            drop(chunk);
            drop(asset);

            // Check if the chunk is already downloaded. If it is, then verify
            // it according to the set method, and if the patch is identified
            // as valid, then push it to the patches list. Otherwise it will be
            // re-downloaded first.
            if patch_info.patch_path.exists() {
                if self.verify_chunks == SophonUpdaterVerifyMethod::None {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        ?patch_info,
                        url = ?chunk_download_url,
                        method = ?self.verify_chunks,
                        "asset chunk already downloaded"
                    );

                    patches.push(patch_info);

                    continue;
                }

                else if patch_info.patch_path.metadata()?.len()
                    == patch_info.patch_size
                {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        ?patch_info,
                        url = ?chunk_download_url,
                        method = ?self.verify_chunks,
                        "asset chunk already downloaded"
                    );

                    patches.push(patch_info);

                    continue;
                }
            }

            // If we cannot fit patch in memory yet AND the tasks queue is not
            // empty - then we wait until already scheduled patches finish
            // downloading.
            if occupied_memory + patch_info.patch_size > self.target_memory_usage
                && !tasks.is_empty()
            {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    tasks = tasks.len(),
                    ?occupied_memory,
                    "wait for scheduled download tasks"
                );

                patches.extend(
                    futures::future::try_join_all(
                        tasks.drain(..)
                            .map(flatten)
                    ).await?
                );

                occupied_memory = 0;
            }

            #[cfg(feature = "tracing")]
            tracing::trace!(
                ?patch_info,
                url = ?chunk_download_url,
                "schedule asset chunk download"
            );

            // For some reason the actual hdiff patch on the server is stored
            // inside of *another file* (???) and you need to make a RANGE
            // HTTP GET request to obtain it. The hdiff patch is also
            // *not compressed* despite what the API will tell you.
            let mut request = self.client.get(&chunk_download_url)
                .header(
                    reqwest::header::RANGE,
                    format!(
                        "bytes={}-{}",
                        chunk_download_offset,
                        chunk_download_offset + patch_info.patch_size - 1
                    )
                );

            if let Some(timeout_per_mb) = self.fetch_chunk_timeout_per_mb {
                request = request.timeout({
                    (patch_info.patch_size as f64 / 1024.0 / 1024.0).ceil() as u32
                        * timeout_per_mb
                });
            }

            occupied_memory += patch_info.patch_size;

            // Start chunk downloading task.
            let future = async move {
                let request_copy = request.try_clone();

                let response = request.send().await?;

                // Verify response size.
                if self.verify_chunks != SophonUpdaterVerifyMethod::None
                    && let Some(content_length) = response.content_length()
                    && content_length != patch_info.patch_size
                {
                    return Err(SophonUpdaterError::ChunkSizeMismatch {
                        url: chunk_download_url,
                        actual: content_length,
                        expected: patch_info.patch_size,
                        offset: chunk_download_offset,
                        length: patch_info.patch_size
                    });
                }

                let mut chunk_body = response.bytes().await;

                if let Some(request) = request_copy && chunk_body.is_err() {
                    for attempt in 1..self.chunk_download_attempts {
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

                let chunk_body = chunk_body?.to_vec();

                // Verify downloaded chunk.
                if self.verify_chunks != SophonUpdaterVerifyMethod::None
                    && chunk_body.len() as u64 != patch_info.patch_size
                {
                    return Err(SophonUpdaterError::ChunkSizeMismatch {
                        url: chunk_download_url,
                        actual: chunk_body.len() as u64,
                        expected: patch_info.patch_size,
                        offset: chunk_download_offset,
                        length: patch_info.patch_size
                    });
                }

                // Write downloaded chunk to disk.
                tokio::fs::write(&patch_info.patch_path, chunk_body).await?;

                Ok::<PatchInfo, SophonUpdaterError>(patch_info)
            };

            let task = match &self.runtime {
                Some(runtime) => runtime.spawn(future),
                None => tokio::spawn(future)
            };

            tasks.push(task);
        }

        // Wait for all the remaining tasks to finish.
        patches.extend(
            futures::future::try_join_all(
                tasks.into_iter()
                    .map(flatten)
            ).await?
        );

        // --------- Stage 3: patch game files.

        Ok(())
    }
}
