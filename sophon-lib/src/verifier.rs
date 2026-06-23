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
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, BufReader};

use md5::{Md5, Digest};

use crate::protos::SophonDownloadAssetsInfoAsset;

struct Asset {
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

pub struct SophonVerifier {
    assets: Box<[Asset]>,
    cache: HashMap<PathBuf, bool>,
    fast_verify: bool
}

impl SophonVerifier {
    pub fn new(
        assets: impl IntoIterator<Item = SophonDownloadAssetsInfoAsset>
    ) -> Self {
        let assets = assets.into_iter()
            .map(|asset| Asset {
                path: asset.path,
                size: asset.size,
                hash_md5: asset.hash_md5
            })
            .collect::<Box<[Asset]>>();

        Self {
            cache: HashMap::with_capacity(assets.len()),
            assets,
            fast_verify: false
        }
    }

    /// Toggle fast verify mode. If enabled, then only files sizes will be
    /// considered during verifications. If disabled, verifier will calculate
    /// hashes to ensure that the files are valid.
    pub fn with_fast_verify(mut self, fast_verify: bool) -> Self {
        self.fast_verify = fast_verify;

        self
    }

    /// Verify given file. If it's not a part of the game, then `false` is
    /// returned.
    pub fn verify_file(&mut self, path: PathBuf) -> std::io::Result<VerifyResult> {
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

        let metadata = path.metadata()?;

        if metadata.len() != asset.size {
            self.cache.insert(path, false);

            return Ok(VerifyResult::Invalid);
        }

        else if self.fast_verify {
            self.cache.insert(path, true);

            return Ok(VerifyResult::Valid);
        }

        let mut file = BufReader::new(File::open(&path)?);
        let mut hasher = Md5::default();

        let mut buf = [0; 1024];

        loop {
            let n = file.read(&mut buf)?;

            if n == 0 {
                break;
            }

            hasher.update(&buf[..n]);
        }

        let hash = hex::encode(hasher.finalize());

        if hash == asset.hash_md5 {
            self.cache.insert(path, true);

            Ok(VerifyResult::Valid)
        }

        else {
            self.cache.insert(path, false);

            Ok(VerifyResult::Invalid)
        }
    }
}
