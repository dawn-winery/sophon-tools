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

use serde_json::Value as Json;

use super::sophon_manifest_info::SophonApiProtobufManifestInfo;
use super::sophon_download_info::SophonApiProtobufDownloadInfo;

#[derive(Debug, Clone, thiserror::Error)]
pub enum SophonApiPackageDownloadInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackageDownloadInfo {
    /// `build_id`.
    ///
    /// Example: `Ymu2ndDuz0Mj`
    pub build_id: String,

    /// `tag`.
    ///
    /// Example: `3.0.0`
    pub version: String,

    /// `manifests`.
    pub manifests: Box<[SophonApiPackageManifest]>
}

impl TryFrom<&Json> for SophonApiPackageDownloadInfo {
    type Error = SophonApiPackageDownloadInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            build_id: value.get("build_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("build_id"))?,

            version: value.get("tag")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("tag"))?,

            manifests: value.get("manifests")
                .and_then(Json::as_array)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests"))
                .and_then(|manifests| {
                    manifests.iter()
                        .map(SophonApiPackageManifest::try_from)
                        .collect::<Result<Box<[_]>, SophonApiPackageDownloadInfoError>>()
                })?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackageManifest {
    /// `manifests[].matching_field`.
    ///
    /// Example: `300002`
    pub name: String,

    /// `manifests[].category_id`.
    ///
    /// Example: `10236`
    pub category_id: String,

    /// `manifests[].category_name`.
    ///
    /// Example: `口型资源▶English`
    pub category_name: String,

    /// `manifests[].manifest`
    pub manifest_info: SophonApiProtobufManifestInfo,

    /// `manifests[].manifest_download`
    pub manifest_download: SophonApiProtobufDownloadInfo,

    /// `manifests[].chunk_download`
    pub chunk_download: SophonApiProtobufDownloadInfo,

    /// `manifests[].stats`
    pub stats: SophonApiPackageManifestStats
}

impl TryFrom<&Json> for SophonApiPackageManifest {
    type Error = SophonApiPackageDownloadInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.get("matching_field")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].matching_field"))?,

            category_id: value.get("category_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].category_id"))?,

            category_name: value.get("category_name")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].category_name"))?,

            manifest_info: value.get("manifest")
                .and_then(|info| SophonApiProtobufManifestInfo::try_from(info).ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].manifest"))?,

            manifest_download: value.get("manifest_download")
                .and_then(|info| SophonApiProtobufDownloadInfo::try_from(info).ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].manifest_download"))?,

            chunk_download: value.get("chunk_download")
                .and_then(|info| SophonApiProtobufDownloadInfo::try_from(info).ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].chunk_download"))?,

            stats: value.get("stats")
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("manifests[].stats"))
                .and_then(SophonApiPackageManifestStats::try_from)?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackageManifestStats {
    /// `compressed_size` - total chunks download size in bytes.
    ///
    /// Example: `55337025812`
    pub compressed_size: u64,

    /// `uncompressed_size` - total chunks size after decompression in bytes.
    ///
    /// Example: `56498483429`
    pub decompressed_size: u64,

    /// `chunk_count` - amount of chunks.
    ///
    /// Example: `53160`
    pub chunks: u64,

    /// `file_count` - amount of files.
    ///
    /// Example: `9789`
    pub files: u64
}

impl TryFrom<&Json> for SophonApiPackageManifestStats {
    type Error = SophonApiPackageDownloadInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            compressed_size: value.get("compressed_size")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("compressed_size"))?,

            decompressed_size: value.get("uncompressed_size")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("uncompressed_size"))?,

            chunks: value.get("chunk_count")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("chunk_count"))?,

            files: value.get("file_count")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiPackageDownloadInfoError::InvalidField("file_count"))?
        })
    }
}
