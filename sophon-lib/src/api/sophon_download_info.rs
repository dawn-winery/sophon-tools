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
pub enum SophonApiProtobufDownloadInfoError {
    #[error("field '{0}' is invalid")]
    InvalidField(&'static str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SophonApiProtobufDownloadInfo {
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

impl TryFrom<&Json> for SophonApiProtobufDownloadInfo {
    type Error = SophonApiProtobufDownloadInfoError;

    fn try_from(value: &Json) -> Result<Self, Self::Error> {
        Ok(Self {
            compressed: value.get("compression")
                .and_then(Json::as_i64)
                .map(|value| value == 1)
                .ok_or(SophonApiProtobufDownloadInfoError::InvalidField("compression"))?,

            encrypted: value.get("encryption")
                .and_then(Json::as_i64)
                .map(|value| value == 1)
                .ok_or(SophonApiProtobufDownloadInfoError::InvalidField("encryption"))?,

            password: value.get("password")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiProtobufDownloadInfoError::InvalidField("password"))?,

            url_prefix: value.get("url_prefix")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiProtobufDownloadInfoError::InvalidField("url_prefix"))?,

            url_suffix: value.get("url_suffix")
                .and_then(Json::as_str)
                .map(String::from)
                .ok_or(SophonApiProtobufDownloadInfoError::InvalidField("url_suffix"))?
        })
    }
}
