use protobuf::Message;
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;

use crate::{
    GameEdition, SophonError,
    api::schemas::{
        ApiResponse,
        game_branches::{GameBranches, PackageInfo},
        game_scan_info::GameScanInfo,
        sophon_diff::{SophonDiff, SophonDiffs},
        sophon_manifests::{SophonDownloadInfo, SophonDownloads},
    },
    protos::{SophonManifest::SophonManifestProto, SophonPatch::SophonPatchProto},
};

pub mod schemas;

// URLs

fn get_game_branches_url(edition: GameEdition) -> String {
    format!(
        "{}/hyp/hyp-connect/api/getGameBranches?launcher_id={}",
        edition.branches_host(),
        edition.launcher_id()
    )
}

fn get_game_scan_info_url(edition: GameEdition) -> String {
    format!(
        "{}/hyp/hyp-connect/api/getGameScanInfo?launcher_id={}",
        edition.branches_host(),
        edition.launcher_id()
    )
}

fn sophon_patch_info_url(package_info: &PackageInfo, edition: GameEdition) -> String {
    format!(
        "{}/downloader/sophon_chunk/api/getPatchBuild?branch={}&password={}&package_id={}",
        edition.api_host(),
        package_info.branch,
        package_info.password,
        package_info.package_id
    )
}

fn sophon_download_info_url(package_info: &PackageInfo, edition: GameEdition) -> String {
    format!(
        "{}/downloader/sophon_chunk/api/getBuild?branch={}&password={}&package_id={}",
        edition.api_host(),
        package_info.branch,
        package_info.password,
        package_info.package_id
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

pub fn get_protobuf_from_url<T: Message>(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<T, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap()
    } else {
        compressed_manifest.into()
    };

    let parsed_manifest = T::parse_from_bytes(&protobuf_bytes).unwrap();

    Ok(parsed_manifest)
}

pub fn get_protobuf_from_url_raw(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<Vec<u8>, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap()
    } else {
        compressed_manifest.into()
    };

    Ok(protobuf_bytes)
}

// Specific API endpoint and datatype getters

pub fn get_game_branches_info(
    client: &Client,
    edition: GameEdition,
) -> Result<GameBranches, SophonError> {
    api_get_request(client, get_game_branches_url(edition))
}

pub fn get_game_branches_info_raw(
    client: &Client,
    edition: GameEdition,
) -> Result<String, SophonError> {
    api_get_request_raw(client, get_game_branches_url(edition))
}

pub fn get_game_scan_info(
    client: &Client,
    edition: GameEdition,
) -> Result<GameScanInfo, SophonError> {
    api_get_request(client, get_game_scan_info_url(edition))
}

pub fn get_game_scan_info_raw(
    client: &Client,
    edition: GameEdition,
) -> Result<String, SophonError> {
    api_get_request_raw(client, get_game_scan_info_url(edition))
}

pub fn get_game_download_sophon_info(
    client: &Client,
    package_info: &PackageInfo,
    edition: GameEdition,
) -> Result<SophonDownloads, SophonError> {
    let url = sophon_download_info_url(package_info, edition);

    api_get_request(client, url)
}

pub fn get_game_download_sophon_info_raw(
    client: &Client,
    package_info: &PackageInfo,
    edition: GameEdition,
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
) -> Result<Vec<u8>, SophonError> {
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
    edition: GameEdition,
) -> Result<SophonDiffs, SophonError> {
    let url = sophon_patch_info_url(package_info, edition);

    api_post_request(client, &url)
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
