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

#[derive(Debug, Clone, thiserror::Error)]
pub enum SophonApiPackagePatchInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackagePatchInfo {
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
    pub manifests: Box<[SophonApiPackagePatchInfoManifest]>
}

impl TryFrom<&Json> for SophonApiPackagePatchInfo {
    type Error = SophonApiPackagePatchInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            build_id: value.get("build_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("build_id"))?,

            patch_id: value.get("patch_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("patch_id"))?,

            version: value.get("tag")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("tag"))?,

            manifests: value.get("manifests")
                .and_then(Json::as_array)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("manifests"))
                .and_then(|manifests| {
                    manifests.iter()
                        .map(SophonApiPackagePatchInfoManifest::try_from)
                        .collect::<Result<Box<[_]>, SophonApiPackagePatchInfoError>>()
                })?
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiPackagePatchInfoManifest {
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
    pub category_name: String
}

impl TryFrom<&Json> for SophonApiPackagePatchInfoManifest {
    type Error = SophonApiPackagePatchInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.get("category_id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("main.categories[].category_id"))?,

            name: value.get("matching_field")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiPackagePatchInfoError::InvalidField("main.categories[].matching_field"))?
        })
    }
}
