use std::borrow::Cow;

use bytes::Bytes;
use prost::Message;
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;

use crate::{
    GameEdition, SophonError,
    api::schemas::{
        ApiResponse,
        game_branches::{GameBranches, PackageInfo},
        game_configs::GameConfigs,
        game_scan_info::GameScanInfo,
        sophon_diff::{SophonDiff, SophonDiffs},
        sophon_manifests::{SophonDownloadInfo, SophonDownloads},
    },
    protos::{SophonManifestProto, SophonPatchProto},
};

pub mod schemas;

/// Convenience struct for keepign the common arguments
#[derive(Debug, Clone)]
pub struct ApiContext {
    pub client: Client,
    pub edition: GameEdition,
}

/// A wrapper for chaining api calls that require a previosu api call result
#[derive(Debug, Clone)]
pub struct ApiContextWrapper<'ctx, T> {
    pub ctx: &'ctx ApiContext,
    pub value: T,
}

pub type ApiResult<'ctx, T> = Result<ApiContextWrapper<'ctx, T>, SophonError>;

macro_rules! api_wrapper {
    ($wrapname:ident => $implname:ident) => {
        api_wrapper! {String: $wrapname => $implname}
    };
    ($restype:ident: $wrapname:ident => $implname:ident) => {
        pub fn $wrapname<'ctx>(&'ctx self) -> ApiResult<'ctx, $restype> {
            self.wrap($implname(&self.client, &self.edition))
        }
    };
}

impl ApiContext {
    fn wrap<T>(&self, v: Result<T, SophonError>) -> ApiResult<'_, T> {
        v.map(|value| ApiContextWrapper { ctx: self, value })
    }

    api_wrapper! {GameBranches: game_branches => get_game_branches_info}
    api_wrapper! {game_branches_raw => get_game_branches_info_raw}
    api_wrapper! {GameScanInfo: game_scan => get_game_scan_info}
    api_wrapper! {game_scan_raw => get_game_scan_info_raw}
    api_wrapper! {GameConfigs: game_configs => get_game_configs}
    api_wrapper! {game_configs_raw => get_game_configs_raw}
}

impl<'ctx, T> ApiContextWrapper<'ctx, T> {
    fn wrap<T2>(&self, v: Result<T2, SophonError>) -> ApiResult<'ctx, T2> {
        self.ctx.wrap(v)
    }

    pub fn map<'a, T2: 'a, F: FnOnce(&'a T) -> T2>(&'a self, f: F) -> ApiContextWrapper<'ctx, T2> {
        ApiContextWrapper {
            ctx: self.ctx,
            value: f(&self.value),
        }
    }
}

impl<'ctx, T> From<ApiContextWrapper<'ctx, Option<T>>> for Option<ApiContextWrapper<'ctx, T>> {
    fn from(value: ApiContextWrapper<'ctx, Option<T>>) -> Self {
        value.value.map(|v| ApiContextWrapper {
            ctx: value.ctx,
            value: v,
        })
    }
}

macro_rules! api_wrapper_val {
    ($wrapname:ident => $implname:ident) => {
        api_wrapper_val! {String: $wrapname => $implname}
    };
    ($restype:ident: $wrapname:ident => $implname:ident) => {
        pub fn $wrapname(&'ctx self) -> ApiResult<'ctx, $restype> {
            self.wrap($implname(&self.ctx.client, &self.value, &self.ctx.edition))
        }
    };
}

impl<'ctx> ApiContextWrapper<'ctx, PackageInfo> {
    api_wrapper_val! {SophonDownloads: downloads => get_game_download_sophon_info}
    api_wrapper_val! {downloads_raw => get_game_download_sophon_info_raw}
    api_wrapper_val! {SophonDiffs: diffs => get_game_diffs_sophon_info}
    api_wrapper_val! {diffs_raw => get_game_diffs_sophon_info_raw}
}

macro_rules! api_wrapper_val_noedition {
    ($wrapname:ident => $implname:ident) => {
        api_wrapper_val_noedition! {Bytes: $wrapname => $implname}
    };
    ($restype:ident: $wrapname:ident => $implname:ident) => {
        pub fn $wrapname(&'ctx self) -> ApiResult<'ctx, $restype> {
            self.wrap($implname(&self.ctx.client, &self.value))
        }
    };
}

impl<'ctx> ApiContextWrapper<'ctx, SophonDownloads> {
    pub fn for_component(
        &self,
        matching_field: &str,
    ) -> Option<ApiContextWrapper<'ctx, &SophonDownloadInfo>> {
        self.map(|downloads| downloads.get_manifests_for(matching_field))
            .into()
    }
}

impl<'ctx> ApiContextWrapper<'ctx, &SophonDownloadInfo> {
    api_wrapper_val_noedition! {SophonManifestProto: download_manifest => get_download_manifest}
    api_wrapper_val_noedition! {download_manifest_raw => get_download_manifest_raw}
}

impl<'ctx> ApiContextWrapper<'ctx, SophonDiffs> {
    pub fn for_component(
        &self,
        matching_field: &str,
    ) -> Option<ApiContextWrapper<'ctx, &SophonDiff>> {
        self.map(|diffs| diffs.get_manifests_for(matching_field))
            .into()
    }
}

impl<'ctx> ApiContextWrapper<'ctx, &SophonDiff> {
    api_wrapper_val_noedition! {SophonPatchProto: patch_manifest => get_patch_manifest}
    api_wrapper_val_noedition! {patch_manifest_raw => get_patch_manifest_raw}
}

// URLs

fn get_game_branches_url(edition: &GameEdition) -> String {
    format!(
        "{}/hyp/hyp-connect/api/getGameBranches?launcher_id={}",
        edition.branches_host(),
        edition.launcher_id()
    )
}

fn get_game_scan_info_url(edition: &GameEdition) -> String {
    format!(
        "{}/hyp/hyp-connect/api/getGameScanInfo?launcher_id={}",
        edition.branches_host(),
        edition.launcher_id()
    )
}

fn get_game_configs_url(edition: &GameEdition) -> String {
    format!(
        "{}/hyp/hyp-connect/api/getGameConfigs?launcher_id={}",
        edition.branches_host(),
        edition.launcher_id()
    )
}

fn sophon_patch_info_url(package_info: &PackageInfo, edition: &GameEdition) -> String {
    format!(
        "{}/downloader/sophon_chunk/api/getPatchBuild?branch={}&password={}&package_id={}&tag={}",
        edition.api_host(),
        package_info.branch,
        package_info.password,
        package_info.package_id,
        package_info.tag
    )
}

fn sophon_download_info_url(package_info: &PackageInfo, edition: &GameEdition) -> String {
    format!(
        "{}/downloader/sophon_chunk/api/getBuild?branch={}&password={}&package_id={}&tag={}",
        edition.api_host(),
        package_info.branch,
        package_info.password,
        package_info.package_id,
        package_info.tag
    )
}

// HTTP API call helpers

fn api_get_request<T: DeserializeOwned>(
    client: &Client,
    url: impl AsRef<str>,
) -> Result<T, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    Ok(response.json::<ApiResponse<T>>()?.data)
}

fn api_post_request<T: DeserializeOwned>(
    client: &Client,
    url: impl AsRef<str>,
) -> Result<T, SophonError> {
    let response = client.post(url.as_ref()).send()?.error_for_status()?;

    Ok(response.json::<ApiResponse<T>>()?.data)
}

fn api_get_request_raw(client: &Client, url: impl AsRef<str>) -> Result<String, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    Ok(response.text()?)
}

fn api_post_request_raw(client: &Client, url: impl AsRef<str>) -> Result<String, SophonError> {
    let response = client.post(url.as_ref()).send()?.error_for_status()?;

    Ok(response.text()?)
}

// Protobuf helpers

pub fn get_protobuf_from_url<T>(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<T, SophonError>
where
    T: Message,
    T: Default,
{
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap()
    } else {
        compressed_manifest.into()
    };

    let parsed_manifest = T::decode(protobuf_bytes.as_slice()).unwrap();

    Ok(parsed_manifest)
}

pub fn get_protobuf_from_url_raw(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<Bytes, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap().into()
    } else {
        compressed_manifest
    };

    Ok(protobuf_bytes)
}

/// Try to decode protobuf from arbitrary data. Tries to decompress the data using zstd, and if
/// that fails, assumes the data is uncompressed
pub fn decode_protobuf<T>(data: &[u8]) -> Result<T, SophonError>
where
    T: Message,
    T: Default,
{
    let mut data_ref = data;
    let decomperssed_data = zstd::decode_all(&mut data_ref)
        .inspect_err(|err| {
            tracing::warn!(
                ?err,
                "Failed to decomperss provided data stream, assuming the data is uncompressed"
            )
        })
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(data));

    T::decode(decomperssed_data.as_ref())
        .map_err(|err| SophonError::IoError(std::io::Error::other(err)))
}

// Specific API endpoint and datatype getters

pub fn get_game_branches_info(
    client: &Client,
    edition: &GameEdition,
) -> Result<GameBranches, SophonError> {
    api_get_request(client, get_game_branches_url(edition))
}

pub fn get_game_branches_info_raw(
    client: &Client,
    edition: &GameEdition,
) -> Result<String, SophonError> {
    api_get_request_raw(client, get_game_branches_url(edition))
}

pub fn get_game_scan_info(
    client: &Client,
    edition: &GameEdition,
) -> Result<GameScanInfo, SophonError> {
    api_get_request(client, get_game_scan_info_url(edition))
}

pub fn get_game_scan_info_raw(
    client: &Client,
    edition: &GameEdition,
) -> Result<String, SophonError> {
    api_get_request_raw(client, get_game_scan_info_url(edition))
}

pub fn get_game_configs(
    client: &Client,
    edition: &GameEdition,
) -> Result<GameConfigs, SophonError> {
    api_get_request(client, get_game_configs_url(edition))
}

pub fn get_game_configs_raw(client: &Client, edition: &GameEdition) -> Result<String, SophonError> {
    api_get_request_raw(client, get_game_configs_url(edition))
}

pub fn get_game_download_sophon_info(
    client: &Client,
    package_info: &PackageInfo,
    edition: &GameEdition,
) -> Result<SophonDownloads, SophonError> {
    let url = sophon_download_info_url(package_info, edition);

    api_get_request(client, url)
}

pub fn get_game_download_sophon_info_raw(
    client: &Client,
    package_info: &PackageInfo,
    edition: &GameEdition,
) -> Result<String, SophonError> {
    let url = sophon_download_info_url(package_info, edition);

    api_get_request_raw(client, url)
}

pub fn get_download_manifest(
    client: &Client,
    download_info: &SophonDownloadInfo,
) -> Result<SophonManifestProto, SophonError> {
    let url_prefix = &download_info.manifest_download.url_prefix;
    let url_suffix = &download_info.manifest_download.url_suffix;
    let manifest_id = &download_info.manifest.id;

    get_protobuf_from_url(
        client,
        format!("{url_prefix}{url_suffix}/{manifest_id}"),
        download_info.manifest_download.compression == 1,
    )
}

pub fn get_download_manifest_raw(
    client: &Client,
    download_info: &SophonDownloadInfo,
) -> Result<Bytes, SophonError> {
    let url_prefix = &download_info.manifest_download.url_prefix;
    let url_suffix = &download_info.manifest_download.url_suffix;
    let manifest_id = &download_info.manifest.id;

    get_protobuf_from_url_raw(
        client,
        format!("{url_prefix}{url_suffix}/{manifest_id}"),
        download_info.manifest_download.compression == 1,
    )
}

pub fn get_game_diffs_sophon_info(
    client: &Client,
    package_info: &PackageInfo,
    edition: &GameEdition,
) -> Result<SophonDiffs, SophonError> {
    let url = sophon_patch_info_url(package_info, edition);

    api_post_request(client, &url)
}

pub fn get_game_diffs_sophon_info_raw(
    client: &Client,
    package_info: &PackageInfo,
    edition: &GameEdition,
) -> Result<String, SophonError> {
    let url = sophon_patch_info_url(package_info, edition);

    api_post_request_raw(client, &url)
}

pub fn get_patch_manifest(
    client: &Client,
    diff_info: &SophonDiff,
) -> Result<SophonPatchProto, SophonError> {
    let url_prefix = &diff_info.manifest_download.url_prefix;
    let url_suffix = &diff_info.manifest_download.url_suffix;
    let manifest_id = &diff_info.manifest.id;

    get_protobuf_from_url(
        client,
        format!("{url_prefix}{url_suffix}/{manifest_id}"),
        diff_info.manifest_download.compression == 1,
    )
}

pub fn get_patch_manifest_raw(
    client: &Client,
    diff_info: &SophonDiff,
) -> Result<Bytes, SophonError> {
    let url_prefix = &diff_info.manifest_download.url_prefix;
    let url_suffix = &diff_info.manifest_download.url_suffix;
    let manifest_id = &diff_info.manifest.id;

    get_protobuf_from_url_raw(
        client,
        format!("{url_prefix}{url_suffix}/{manifest_id}"),
        diff_info.manifest_download.compression == 1,
    )
}
