use std::collections::BTreeMap;

use bytes::Bytes;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use super::sophon_manifests::{DownloadInfo, Manifest, ManifestStats};
use crate::{SophonError, api::ApiContextWrapper, protos::SophonPatchProto};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SophonDiffs {
    pub build_id: String,
    pub patch_id: String,
    pub tag: String,
    pub manifests: Vec<SophonDiff>,
}

impl SophonDiffs {
    /// `matching_field` is usually either `game` or one of the voiceover
    /// language options
    pub fn get_manifests_for(&self, matching_field: &str) -> Option<&SophonDiff> {
        self.manifests
            .iter()
            .find(|manifest| manifest.matching_field == matching_field)
    }
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SophonDiff {
    pub category_id: String,
    pub category_name: String,
    pub matching_field: String,
    pub manifest: Manifest,
    pub diff_download: DownloadInfo,
    pub manifest_download: DownloadInfo,
    pub stats: BTreeMap<String, ManifestStats>,
}

impl SophonDiff {
    fn manifest_url(&self) -> String {
        let url_prefix = &self.manifest_download.url_prefix;
        let url_suffix = &self.manifest_download.url_suffix;
        let manifest_id = &self.manifest.id;

        format!("{url_prefix}{url_suffix}/{manifest_id}")
    }

    /// Recommended to use [`ApiContextWrapper::patch_manifest`] instead
    pub fn patch_manifest(&self, client: &Client) -> Result<SophonPatchProto, SophonError> {
        crate::api::get_protobuf_from_url(
            client,
            self.manifest_url(),
            self.manifest_download.compression == 1,
        )
    }

    /// Recommended to use [`ApiContextWrapper::patch_manifest_raw`] instead
    pub fn patch_manifest_raw(&self, client: &Client) -> Result<Bytes, SophonError> {
        crate::api::get_protobuf_from_url_raw(
            client,
            self.manifest_url(),
            self.manifest_download.compression == 1,
        )
    }
}

impl<'ctx> ApiContextWrapper<'ctx, &SophonDiff> {
    crate::api::api_wrappers_forward! {
        patch_manifest: SophonPatchProto,
        patch_manifest_raw: Bytes
    }
}
