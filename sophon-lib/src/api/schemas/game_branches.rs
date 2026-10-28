use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{
    api::{
        ApiContext, ApiContextWrapper,
        schemas::package::{PackageInfo, PackageIter},
    },
    utils::version::Version,
};

#[derive(Debug)]
pub struct GameBranchesIter<'a, 'b> {
    id_or_biz: &'a str,
    version: Option<&'a str>,
    iter: std::slice::Iter<'b, GameBranchInfo>,
}

impl<'a, 'b> Iterator for GameBranchesIter<'a, 'b> {
    type Item = &'b GameBranchInfo;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find(|branch| {
            (branch.game.id == self.id_or_biz || branch.game.biz == self.id_or_biz)
                && branch
                    .main
                    .as_ref()
                    .or(branch.pre_download.as_ref())
                    .map(|package_info| {
                        if let Some(v) = self.version {
                            package_info.tag == v
                        } else {
                            true
                        }
                    })
                    .unwrap_or(false)
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameBranches {
    pub game_branches: Vec<GameBranchInfo>,
}

impl ApiContext {
    fn game_branches_url(&self) -> String {
        format!(
            "{}/hyp/hyp-connect/api/getGameBranches?launcher_id={}",
            self.edition.branches_host(),
            self.edition.launcher_id()
        )
    }

    crate::api::api_methods! {
        game_branches(get, url = game_branches_url()) -> GameBranches,
        game_branches_raw(get, raw, url = game_branches_url()) -> String
    }
}

impl GameBranches {
    pub fn get_game_branches_by_id_or_biz<'a, 'b>(
        &'b self,
        id_or_biz: &'a str,
        version: Option<&'a str>,
    ) -> GameBranchesIter<'a, 'b> {
        GameBranchesIter {
            id_or_biz,
            version,
            iter: self.game_branches.iter(),
        }
    }

    pub fn get_game_branch_by_id_or_biz_latest(&self, id_or_biz: &str) -> Option<&GameBranchInfo> {
        self.get_game_branches_by_id_or_biz(id_or_biz, None)
            .max_by_key(|branch| {
                &branch
                    .main
                    .as_ref()
                    .or(branch.pre_download.as_ref())
                    .expect("empty branches filtered out")
                    .tag
            })
    }

    pub fn get_packages_by_id_or_biz<'a, 'b>(
        &'b self,
        id_or_biz: &'a str,
        version: Option<&'a str>,
        preload: bool,
    ) -> PackageIter<'a, 'b> {
        PackageIter {
            branches_iter: GameBranchesIter {
                id_or_biz,
                version,
                iter: self.game_branches.iter(),
            },
            preload,
        }
    }

    pub fn get_package_by_id_or_biz_latest(
        &self,
        id_or_biz: &str,
        preload: bool,
    ) -> Option<&PackageInfo> {
        self.get_packages_by_id_or_biz(id_or_biz, None, preload)
            .max_by_key(|package| &package.tag)
    }
}

impl<'ctx, 'a> ApiContextWrapper<'ctx, &'a GameBranches> {
    pub fn branch_latest(
        &self,
        id_or_biz: &str,
    ) -> Option<ApiContextWrapper<'ctx, &'a GameBranchInfo>> {
        self.map(|branches| branches.get_game_branch_by_id_or_biz_latest(id_or_biz))
            .into()
    }

    pub fn branch_version(
        &self,
        id_or_biz: &str,
        version: &str,
    ) -> Option<ApiContextWrapper<'ctx, &'a GameBranchInfo>> {
        self.map(|branches| {
            branches
                .get_game_branches_by_id_or_biz(id_or_biz, Some(version))
                .next()
        })
        .into()
    }

    pub fn branch_latest_or_version(
        &self,
        id_or_biz: &str,
        version: Option<&str>,
    ) -> Option<ApiContextWrapper<'ctx, &'a GameBranchInfo>> {
        version
            .map(|version| self.branch_version(id_or_biz, version))
            .unwrap_or_else(|| self.branch_latest(id_or_biz))
    }

    pub fn branches_iter<'b>(
        &self,
        id_or_biz: &'b str,
        version: Option<&'b str>,
    ) -> ApiContextWrapper<'ctx, GameBranchesIter<'b, 'a>> {
        self.map(|branches| branches.get_game_branches_by_id_or_biz(id_or_biz, version))
    }
}

impl<'ctx, 'a> ApiContextWrapper<'ctx, &'a GameBranchInfo> {
    pub fn main(self) -> Option<ApiContextWrapper<'ctx, &'a PackageInfo>> {
        self.map(|branch| branch.main.as_ref()).into()
    }

    pub fn pre_download(self) -> Option<ApiContextWrapper<'ctx, &'a PackageInfo>> {
        self.map(|branch| branch.pre_download.as_ref()).into()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameBranchInfo {
    pub game: Game,
    pub main: Option<PackageInfo>,
    pub pre_download: Option<PackageInfo>,
}

impl GameBranchInfo {
    pub fn version(&self) -> Option<Version> {
        Version::from_str(&self.main.as_ref()?.tag).ok()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Game {
    pub id: String,
    pub biz: String,
}
