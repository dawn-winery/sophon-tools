use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{
    GameEdition,
    api::{
        ApiContextWrapper, ApiContextWrapperIter,
        schemas::{
            game_branches::{GameBranches, GameBranchesIter},
            sophon_diff::SophonDiffs,
            sophon_manifests::SophonDownloads,
        },
    },
    utils::version::Version,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageInfo {
    pub package_id: String,
    pub branch: String,
    pub password: String,
    pub tag: String,
    pub diff_tags: Vec<String>,
    pub categories: Vec<PackageCategory>,
}

#[derive(Debug)]
pub struct PackageIter<'a, 'b> {
    pub(crate) branches_iter: GameBranchesIter<'a, 'b>,
    pub(crate) preload: bool,
}

impl<'a, 'b> Iterator for PackageIter<'a, 'b> {
    type Item = &'b PackageInfo;

    fn next(&mut self) -> Option<Self::Item> {
        self.branches_iter.find_map(|branch_info| {
            if self.preload {
                branch_info.pre_download.as_ref()
            } else {
                branch_info.main.as_ref()
            }
        })
    }
}

impl PackageInfo {
    pub fn version(&self) -> Option<Version> {
        Version::from_str(&self.tag).ok()
    }

    fn downloads_url(&self, edition: &GameEdition) -> String {
        format!(
            "{}/downloader/sophon_chunk/api/getBuild?branch={}&password={}&package_id={}&tag={}",
            edition.api_host(),
            self.branch,
            self.password,
            self.package_id,
            self.tag
        )
    }

    fn diffs_url(&self, edition: &GameEdition) -> String {
        format!(
            "{}/downloader/sophon_chunk/api/getPatchBuild?branch={}&password={}&package_id={}&tag={}",
            edition.api_host(),
            self.branch,
            self.password,
            self.package_id,
            self.tag
        )
    }
}

impl<'ctx> ApiContextWrapper<'ctx, &PackageInfo> {
    fn downloads_url(&self) -> String {
        self.value.downloads_url(&self.ctx.edition)
    }

    fn diffs_url(&self) -> String {
        self.value.diffs_url(&self.ctx.edition)
    }

    crate::api::api_wrapper_methods! {
        downloads(get, url = downloads_url()) -> SophonDownloads,
        downloads_raw(get, raw, url = downloads_url()) -> String,
        diffs(post, url = diffs_url()) -> SophonDiffs,
        diffs_raw(post, url = diffs_url()) -> String
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageCategory {
    pub category_id: String,
    pub matching_field: String,
}

impl<'ctx> ApiContextWrapper<'ctx, GameBranches> {
    pub fn package_latest(
        &self,
        id_or_biz: &str,
        preload: bool,
    ) -> Option<ApiContextWrapper<'ctx, &PackageInfo>> {
        self.map(|branches| branches.get_package_by_id_or_biz_latest(id_or_biz, preload))
            .into()
    }

    pub fn package_version(
        &self,
        id_or_biz: &str,
        version: &str,
        preload: bool,
    ) -> Option<ApiContextWrapper<'ctx, &PackageInfo>> {
        self.map(|branches| {
            branches
                .get_packages_by_id_or_biz(id_or_biz, Some(version), preload)
                .next()
        })
        .into()
    }

    pub fn package_latest_or_version(
        &self,
        id_or_biz: &str,
        version: Option<&str>,
        preload: bool,
    ) -> Option<ApiContextWrapper<'ctx, &PackageInfo>> {
        version
            .map(|version| self.package_version(id_or_biz, version, preload))
            .unwrap_or_else(|| self.package_latest(id_or_biz, preload))
    }

    pub fn packages_iter<'a>(
        &'a self,
        id_or_biz: &'a str,
        version: Option<&'a str>,
        preload: bool,
    ) -> ApiContextWrapperIter<'ctx, PackageIter<'a, 'a>> {
        self.map_iter(|branches| branches.get_packages_by_id_or_biz(id_or_biz, version, preload))
    }
}
