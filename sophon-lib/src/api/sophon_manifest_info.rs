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
pub enum SophonApiProtobufManifestInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiProtobufManifestInfo {
    /// `id` - unique sophon manifest identifier.
    ///
    /// Example: `manifest_968b4a15fe843900_8f4f6753b60854895d2a561e218ee0d5`
    pub id: String,

    /// `checksum` - md5 hash of the downloaded manifest.
    ///
    /// Example: `8f4f6753b60854895d2a561e218ee0d5`
    pub hash_md5: String,

    /// `compressed_size` - compressed manifest size in bytes.
    ///
    /// Example: `2936`
    pub compressed_size: u64,

    /// `uncompressed_size` - decompressed manifest size in bytes.
    ///
    /// Example: `14083`
    pub decompressed_size: u64
}

impl TryFrom<&Json> for SophonApiProtobufManifestInfo {
    type Error = SophonApiProtobufManifestInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.get("id")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiProtobufManifestInfoError::InvalidField("id"))?,

            hash_md5: value.get("checksum")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiProtobufManifestInfoError::InvalidField("checksum"))?,

            compressed_size: value.get("compressed_size")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiProtobufManifestInfoError::InvalidField("compressed_size"))?,

            decompressed_size: value.get("uncompressed_size")
                .and_then(Json::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SophonApiProtobufManifestInfoError::InvalidField("uncompressed_size"))?
        })
    }
}
