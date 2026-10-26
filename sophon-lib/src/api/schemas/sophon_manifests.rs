use bytes::Bytes;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use crate::{SophonError, api::ApiContextWrapper, protos::SophonManifestProto};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SophonDownloads {
    pub build_id: String,
    pub tag: String,
    pub manifests: Vec<SophonDownloadInfo>,
}

impl SophonDownloads {
    /// `matching_field` is usually either `game` or one of the voiceover
    /// language options
    pub fn get_manifests_for(&self, matching_field: &str) -> Option<&SophonDownloadInfo> {
        self.manifests
            .iter()
            .find(|manifest| manifest.matching_field == matching_field)
    }
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SophonDownloadInfo {
    pub category_id: String,
    pub category_name: String,
    pub matching_field: String,
    pub manifest: Manifest,
    pub chunk_download: DownloadInfo,
    pub manifest_download: DownloadInfo,
    pub stats: ManifestStats,
    pub deduplicated_stats: ManifestStats,
}

impl SophonDownloadInfo {
    fn manifest_url(&self) -> String {
        let url_prefix = &self.manifest_download.url_prefix;
        let url_suffix = &self.manifest_download.url_suffix;
        let manifest_id = &self.manifest.id;
        format!("{url_prefix}{url_suffix}/{manifest_id}")
    }

    fn download_manifest(&self, client: &Client) -> Result<SophonManifestProto, SophonError> {
        let url = self.manifest_url();
        crate::api::get_protobuf_from_url(client, url, self.manifest_download.compression == 1)
    }

    fn download_manifest_raw(&self, client: &Client) -> Result<Bytes, SophonError> {
        let url = self.manifest_url();
        crate::api::get_protobuf_from_url_raw(client, url, self.manifest_download.compression == 1)
    }
}

impl<'ctx> ApiContextWrapper<'ctx, &SophonDownloadInfo> {
    crate::api::api_wrappers_forward! {
        download_manifest: SophonManifestProto,
        download_manifest_raw: Bytes
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub id: String,
    pub checksum: String,
    pub compressed_size: String,
    pub uncompressed_size: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadInfo {
    pub encryption: u8,
    pub password: String,
    pub compression: u8,
    pub url_prefix: String,
    pub url_suffix: String,
}

impl DownloadInfo {
    pub fn download_url(&self, id: &str) -> String {
        format!("{}{}/{id}", self.url_prefix, self.url_suffix)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestStats {
    pub compressed_size: String,
    pub uncompressed_size: String,
    pub file_count: String,
    pub chunk_count: String,
}
