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
pub enum SophonApiPackageUpdateInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackageUpdateInfo {
    /// `build_id`.
    ///
    /// Example: `Ymu2ndDuz0Mj`
    pub build_id: String,

    /// `patch_id`.
    ///
    /// Example: `4Kv6PhCiNrcC`
    pub patch_id: String,

    /// `tag`.
    ///
    /// Example: `3.0.0`
    pub version: String,

    /// `manifests`.
    pub manifests: Box<[SophonApiPackageManifest]>
}

impl TryFrom<&Json> for SophonApiPackageUpdateInfo {
    type Error = SophonApiPackageUpdateInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            build_id: value.get("build_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("build_id"))?,

            patch_id: value.get("patch_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("patch_id"))?,

            version: value.get("tag")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("tag"))?,

            manifests: value.get("manifests")
                .and_then(Json::as_array)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests"))
                .and_then(|manifests| {
                    manifests.iter()
                        .map(SophonApiPackageManifest::try_from)
                        .collect::<Result<Box<[_]>, SophonApiPackageUpdateInfoError>>()
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

    /// `manifests[].diff_download`
    pub diff_download: SophonApiProtobufDownloadInfo
}

impl TryFrom<&Json> for SophonApiPackageManifest {
    type Error = SophonApiPackageUpdateInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.get("matching_field")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].matching_field"))?,

            category_id: value.get("category_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].category_id"))?,

            category_name: value.get("category_name")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].category_name"))?,

            manifest_info: value.get("manifest")
                .and_then(|info| SophonApiProtobufManifestInfo::try_from(info).ok())
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].manifest"))?,

            manifest_download: value.get("manifest_download")
                .and_then(|info| SophonApiProtobufDownloadInfo::try_from(info).ok())
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].manifest_download"))?,

            diff_download: value.get("diff_download")
                .and_then(|info| SophonApiProtobufDownloadInfo::try_from(info).ok())
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("manifests[].diff_download"))?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackageManifestDownloadInfo {
    /// `compression` - whether the protobuf file is compressed.
    ///
    /// Example: `1`
    pub compressed: bool,

    /// `encryption` - whether the protobuf file is encrypted.
    ///
    /// Example: `0`
    pub encrypted: bool,

    /// `password` - protobuf encryption password. Empty on public API endpoint.
    ///
    /// Example: `""`
    pub password: String,

    /// `url_prefix` - URL to the protobuf.
    ///
    /// Example: `https://autopatchos.zenlesszonezero.com/pclauncher/diffs/cxi9qfgtcu0w/20260529/3.0.0/8RuEeohLeVa2/10236`
    pub url_prefix: String,

    /// `url_suffix` - URL suffix to the protobuf (?). Empty on public API
    /// endpoint.
    ///
    /// Example: `""`
    pub url_suffix: String
}

impl TryFrom<&Json> for SophonApiPackageManifestDownloadInfo {
    type Error = SophonApiPackageUpdateInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            compressed: value.get("compression")
                .and_then(Json::as_i64)
                .map(|value| value == 1)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("compression"))?,

            encrypted: value.get("encryption")
                .and_then(Json::as_i64)
                .map(|value| value == 1)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("encryption"))?,

            password: value.get("password")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("password"))?,

            url_prefix: value.get("url_prefix")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("url_prefix"))?,

            url_suffix: value.get("url_suffix")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackageUpdateInfoError::InvalidField("url_suffix"))?
        })
    }
}
