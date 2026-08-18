// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@dawn.wine>
//                     "John the Cooling Fan" <ivan8215145640@gmail.com>
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct SophonApiResponse<T> {
    #[serde(rename = "retcode")]
    pub code: i64,
    pub message: String,
    pub data: Option<T>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameIdentifiers {
    /// `game.id` - unique identifier of the game.
    ///
    /// Example: `U5hbdsT9W7`
    #[serde(rename = "id")]
    pub game_id: String,

    /// `game.biz` - human-readable identifier of the game. May not be unique.
    ///
    /// Example: `nap_global`
    #[serde(rename = "biz")]
    pub game_biz: String
}

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct ProtobufManifestInfo {
    /// `id` - unique sophon manifest identifier.
    ///
    /// Example: `manifest_968b4a15fe843900_8f4f6753b60854895d2a561e218ee0d5`
    pub id: String,

    /// `checksum` - md5 hash of the downloaded manifest.
    ///
    /// Example: `8f4f6753b60854895d2a561e218ee0d5`
    #[serde(rename = "checksum")]
    pub hash_md5: String,

    /// `compressed_size` - compressed manifest size in bytes.
    ///
    /// Example: `2936`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub compressed_size: u64,

    /// `uncompressed_size` - decompressed manifest size in bytes.
    ///
    /// Example: `14083`
    #[serde(rename = "uncompressed_size")]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub decompressed_size: u64
}

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct ChunksDownloadInfo {
    /// `compression` - are chunks compressed.
    ///
    /// Example: `1`
    #[serde(rename = "compression")]
    #[serde_as(as = "serde_with::BoolFromInt")]
    pub compressed: bool,

    /// `encryption` - are chunks encrypted.
    ///
    /// Example: `0`
    #[serde(rename = "encryption")]
    #[serde_as(as = "serde_with::BoolFromInt")]
    pub encrypted: bool,

    /// `password` - chunks encryption password (?). Empty on public API
    /// endpoint.
    ///
    /// Example: `""`
    pub password: String,

    /// `url_prefix` - URL to the protobuf.
    ///
    /// Example: `https://autopatchos.zenlesszonezero.com/pclauncher/diffs/cxi9qfgtcu0w/20260529/3.0.0/8RuEeohLeVa2/10236`
    pub url_prefix: String,

    /// `url_suffix` - URL suffix to the protobuf (?). Empty on public API endpoint.
    ///
    /// Example: `""`
    pub url_suffix: String
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GamesBranchesResponse {
    #[serde(rename = "game_branches")]
    pub values: Box<[GamePackageInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GamePackageInfo {
    /// Game identifiers.
    pub game: GameIdentifiers,

    /// Game branch info.
    #[serde(rename = "main")]
    pub branch: GameBranchInfo
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameBranchInfo {
    /// `main.package_id`
    ///
    /// Example: `qsoIbfMm4x`
    pub package_id: String,

    /// `main.branch`. Always "main" on the public API endpoint.
    ///
    /// Example: `main`
    #[serde(rename = "branch")]
    pub branch_name: String,

    /// `main.password`
    ///
    /// Example: `lh0OmNjbhG9x`
    pub password: String,

    /// `main.tag` - latest available version of the game.
    ///
    /// Example: `3.0.0`
    #[serde(rename = "tag")]
    pub version: String,

    /// `main.diff_tags` - list of versions from which the game can be updated.
    ///
    /// Example: `["2.8.0", "2.7.0"]`
    #[serde(rename = "diff_tags")]
    pub diff_versions: Box<[String]>,

    /// `main.categories` - list of game components. Can be base game files,
    /// voice packages, etc.
    pub categories: Box<[GameCategoryInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameCategoryInfo {
    /// `main.categories[].category_id` - per-game category identifier.
    ///
    /// Example: `10176`
    #[serde(rename = "category_id")]
    pub id: String,

    /// `main.categories[].matching_field` - name of the category.
    ///
    /// Example: `game`
    #[serde(rename = "matching_field")]
    pub name: String
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameVersionsResponse {
    #[serde(rename = "game_scan_info")]
    pub values: Box<[GameVersionsInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameVersionsInfo {
    /// `game_id` - unique identifier of the game.
    ///
    /// Example: `U5hbdsT9W7`
    pub game_id: String,

    /// `game_exe_list` - list of all the game versions and md5 hashes of the
    /// game binaries for these versions.
    #[serde(rename = "game_exe_list")]
    pub versions: Box<[GameVersionInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameVersionInfo {
    /// `game_exe_list[].version` - version of the game.
    ///
    /// Example: `3.0.0`
    pub version: String,

    /// `game_exe_list[].md5` - md5 hash of the game binary.
    ///
    /// Example: `eb8ee0dd5c5a7dd9aec6569182886539`
    #[serde(rename = "md5")]
    pub hash_md5: String
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GamesConfigsResponse {
    #[serde(rename = "launch_configs")]
    pub values: Box<[GameConfigInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct GameConfigInfo {
    pub game: GameIdentifiers,

    /// `installation_dir` - name of the directory in which the game should be
    /// installed.
    ///
    /// Example: `ZenlessZoneZero Game`
    #[serde(rename = "installation_dir")]
    pub directory_name: String,

    /// `exe_file_name` - name of the main game binary.
    ///
    /// Example: `ZenlessZoneZero.exe`
    #[serde(rename = "exe_file_name")]
    pub binary_name: String
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct DownloadGameResponse {
    /// `build_id`.
    ///
    /// Example: `Ymu2ndDuz0Mj`
    pub build_id: String,

    /// `tag`.
    ///
    /// Example: `3.0.0`
    #[serde(rename = "tag")]
    pub version: String,

    /// `manifests`.
    pub manifests: Box<[DownloadGameComponentInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct DownloadGameComponentInfo {
    /// `manifests[].matching_field`.
    ///
    /// Example: `300002`
    #[serde(rename = "matching_field")]
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
    #[serde(rename = "manifest")]
    pub manifest_info: ProtobufManifestInfo,

    /// `manifests[].manifest_download`
    pub manifest_download: ChunksDownloadInfo,

    /// `manifests[].chunk_download`
    pub chunk_download: ChunksDownloadInfo,

    /// `manifests[].stats`
    pub stats: DownloadGameComponentStats
}

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct DownloadGameComponentStats {
    /// `compressed_size` - total chunks download size in bytes.
    ///
    /// Example: `55337025812`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub compressed_size: u64,

    /// `uncompressed_size` - total chunks size after decompression in bytes.
    ///
    /// Example: `56498483429`
    #[serde(rename = "uncompressed_size")]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub decompressed_size: u64,

    /// `chunk_count` - amount of chunks.
    ///
    /// Example: `53160`
    #[serde(rename = "chunk_count")]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub chunks: u64,

    /// `file_count` - amount of files.
    ///
    /// Example: `9789`
    #[serde(rename = "file_count")]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub files: u64
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct UpdateGameResponse {
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
    #[serde(rename = "tag")]
    pub version: String,

    /// `manifests`.
    pub manifests: Box<[UpdateGameComponentInfo]>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize)]
pub struct UpdateGameComponentInfo {
    /// `manifests[].matching_field`.
    ///
    /// Example: `300002`
    #[serde(rename = "matching_field")]
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
    #[serde(rename = "manifest")]
    pub manifest_info: ProtobufManifestInfo,

    /// `manifests[].manifest_download`
    pub manifest_download: ChunksDownloadInfo,

    /// `manifests[].diff_download`
    pub diff_download: ChunksDownloadInfo
}
