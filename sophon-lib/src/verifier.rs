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
use std::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;
use std::collections::{HashMap, VecDeque};

use tokio::sync::RwLock;
use tokio::fs::File;
use tokio::io::{BufReader, AsyncReadExt};

use md5::{Md5, Digest};

use crate::protos::{SophonDownloadAssetsInfoAsset, SophonUpdateAssetsInfoAsset};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonVerifierAsset {
    pub path: String,
    pub size: u64,
    pub hash_md5: String
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VerifyResult {
    /// File is valid.
    Valid,

    /// File is invalid.
    Invalid,

    /// Verifier doesn't have information about this file.
    Unknown
}

// TODO: I don't actually need to know PathBuf so I could use hashes of
//       paths to save up space. I could also use specialized HashMap
//       library for faster u64-indexed maps lookups.

pub struct SophonVerifier {
    runtime: Option<tokio::runtime::Handle>,
    assets: Box<[SophonVerifierAsset]>,
    cache: HashMap<PathBuf, bool>,
    fast_verify: bool
}

impl From<Vec<SophonDownloadAssetsInfoAsset>> for SophonVerifier {
    fn from(value: Vec<SophonDownloadAssetsInfoAsset>) -> Self {
        Self {
            runtime: None,
            cache: HashMap::with_capacity(value.len()),
            assets: value.into_iter()
                .map(|asset| SophonVerifierAsset {
                    path: asset.path,
                    size: asset.size,
                    hash_md5: asset.hash_md5
                })
                .collect(),
            fast_verify: false
        }
    }
}

impl From<Vec<SophonUpdateAssetsInfoAsset>> for SophonVerifier {
    fn from(value: Vec<SophonUpdateAssetsInfoAsset>) -> Self {
        Self {
            runtime: None,
            cache: HashMap::with_capacity(value.len()),
            assets: value.into_iter()
                .map(|asset| SophonVerifierAsset {
                    path: asset.path,
                    size: asset.size,
                    hash_md5: asset.hash_md5
                })
                .collect(),
            fast_verify: false
        }
    }
}

impl SophonVerifier {
    pub fn new(assets: Box<[SophonVerifierAsset]>) -> Self {
        Self {
            runtime: None,
            cache: HashMap::with_capacity(assets.len()),
            assets,
            fast_verify: false
        }
    }

    /// Tokio runtime handle to use for async operations.
    ///
    /// If unset, then `tokio::spawn` function will be used to schedule tasks.
    pub fn with_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.runtime = Some(runtime);

        self
    }

    /// Toggle fast verify mode. If enabled, then only files sizes will be
    /// considered during verifications. If disabled, verifier will calculate
    /// hashes to ensure that the files are valid.
    pub fn with_fast_verify(mut self, fast_verify: bool) -> Self {
        self.fast_verify = fast_verify;

        self
    }

    /// Get list of assets info stored in the verifier.
    #[inline]
    pub fn assets(&self) -> impl Iterator<Item = &'_ SophonVerifierAsset> {
        self.assets.iter()
    }

    /// Clear verifications results cache.
    #[inline]
    pub fn clear(&mut self) {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            entries = self.cache.len(),
            "clear assets verifier cache"
        );

        self.cache.clear();
    }

    /// Populate verifier cache with given directory entries and their
    /// verification results. Following `verify_file` calls will read the result
    /// without performing any extra computations.
    pub async fn scan_directory(
        &mut self,
        path: PathBuf,
        updater: Box<dyn Fn(u64, u64, PathBuf) + Send + Sync>
    ) -> std::io::Result<()> {
        let mut entries = VecDeque::from([(path, 0)]);

        while let Some((path, _)) = entries.pop_back() && path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                let path = entry?.path();
                let metadata = tokio::fs::metadata(&path).await?;

                if path.is_dir() {
                    entries.push_back((path, 0));
                } else {
                    entries.push_front((path, metadata.len()));
                }
            }
        }

        if entries.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::from(entries);

        // Large files should be tested last.
        #[allow(clippy::unnecessary_sort_by)]
        entries.sort_by(|a, b| b.1.cmp(&a.1));

        let progress_total = entries.iter()
            .map(|(_, size)| *size)
            .sum::<u64>();

        let progress_current = Arc::new(AtomicU64::new(0));
        let progress_updater = Arc::new(RwLock::new(updater));

        let tasks_size = self.runtime.as_ref()
            .map(|runtime| runtime.metrics().num_workers())
            .or_else(|| {
                std::thread::available_parallelism()
                    .ok()
                    .map(usize::from)
            })
            .unwrap_or(4)
            .max(1);

        let mut tasks = Vec::with_capacity(tasks_size);

        async fn flatten<T>(
            task: tokio::task::JoinHandle<std::io::Result<T>>
        ) -> std::io::Result<T> {
            match task.await {
                Ok(result) => result,
                Err(err) => Err(std::io::Error::other(err))
            }
        }

        while let Some((path, size)) = entries.pop() {
            // Skip already scanned files.
            if self.cache.contains_key(&path) {
                let current = progress_current.fetch_add(
                    size,
                    Ordering::Relaxed
                );

                (progress_updater.read().await)(
                    current + size,
                    progress_total,
                    path
                );

                continue;
            }

            // Find asset info for the given file, or skip it if the file is
            // unknown.
            let Some(asset) = self.assets.iter()
                .find(|asset| path.ends_with(&asset.path))
                .cloned()
            else {
                let current = progress_current.fetch_add(
                    size,
                    Ordering::Relaxed
                );

                (progress_updater.read().await)(
                    current + size,
                    progress_total,
                    path
                );

                continue;
            };

            if tasks.len() >= tasks_size {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    tasks = tasks.len(),
                    "wait for scheduled files verifing tasks"
                );

                let results = futures::future::try_join_all(
                    tasks.drain(..)
                        .map(flatten)
                ).await?;

                for (path, result) in results {
                    match result {
                        VerifyResult::Valid => self.cache.insert(path, true),
                        VerifyResult::Invalid => self.cache.insert(path, false),
                        VerifyResult::Unknown => continue
                    };
                }
            }

            #[cfg(feature = "tracing")]
            tracing::trace!(?path, "schedule file verifying");

            let progress_current = progress_current.clone();
            let progress_updater = progress_updater.clone();

            let fast_verify = self.fast_verify;

            let future = async move {
                let metadata = tokio::fs::metadata(&path).await?;

                if metadata.len() != asset.size {
                    let current = progress_current.fetch_add(
                        size,
                        Ordering::Relaxed
                    );

                    (progress_updater.read().await)(
                        current + size,
                        progress_total,
                        path.clone()
                    );

                    return Ok((path, VerifyResult::Invalid));
                }

                else if fast_verify {
                    let current = progress_current.fetch_add(
                        size,
                        Ordering::Relaxed
                    );

                    (progress_updater.read().await)(
                        current + size,
                        progress_total,
                        path.clone()
                    );

                    return Ok((path, VerifyResult::Valid));
                }

                let mut file = BufReader::with_capacity(
                    64 * 1024,
                    File::open(&path).await?
                );

                let mut hasher = Md5::default();

                let mut buf = [0; 4096];

                loop {
                    let n = file.read(&mut buf).await?;

                    if n == 0 {
                        break;
                    }

                    hasher.update(&buf[..n]);
                }

                let current = progress_current.fetch_add(
                    size,
                    Ordering::Relaxed
                );

                (progress_updater.read().await)(
                    current + size,
                    progress_total,
                    path.clone()
                );

                if hex::encode(hasher.finalize()) == asset.hash_md5 {
                    Ok((path, VerifyResult::Valid))
                } else {
                    Ok((path, VerifyResult::Invalid))
                }
            };

            let task = match &self.runtime {
                Some(runtime) => runtime.spawn(future),
                None => tokio::spawn(future)
            };

            tasks.push(task);
        }

        let results = futures::future::try_join_all(
            tasks.drain(..)
                .map(flatten)
        ).await?;

        for (path, result) in results {
            match result {
                VerifyResult::Valid => self.cache.insert(path, true),
                VerifyResult::Invalid => self.cache.insert(path, false),
                VerifyResult::Unknown => continue
            };
        }

        Ok(())
    }

    /// Verify given file. If it's not a part of the game, then `false` is
    /// returned.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = tracing::Level::DEBUG, skip(self), ret)
    )]
    pub async fn verify_file(
        &mut self,
        path: PathBuf
    ) -> std::io::Result<VerifyResult> {
        if let Some(is_valid) = self.cache.get(&path) {
            match is_valid {
                true => return Ok(VerifyResult::Valid),
                false => return Ok(VerifyResult::Invalid)
            }
        }

        let Some(asset) = self.assets.iter()
            .find(|asset| path.ends_with(&asset.path))
        else {
            return Ok(VerifyResult::Unknown);
        };

        let metadata = tokio::fs::metadata(&path).await?;

        if metadata.len() != asset.size {
            self.cache.insert(path, false);

            return Ok(VerifyResult::Invalid);
        }

        else if self.fast_verify {
            self.cache.insert(path, true);

            return Ok(VerifyResult::Valid);
        }

        let mut file = BufReader::with_capacity(
            64 * 1024,
            File::open(&path).await?
        );

        let mut hasher = Md5::default();

        let mut buf = [0; 4096];

        loop {
            let n = file.read(&mut buf).await?;

            if n == 0 {
                break;
            }

            hasher.update(&buf[..n]);
        }

        if hex::encode(hasher.finalize()) == asset.hash_md5 {
            self.cache.insert(path, true);

            Ok(VerifyResult::Valid)
        }

        else {
            self.cache.insert(path, false);

            Ok(VerifyResult::Invalid)
        }
    }
}
