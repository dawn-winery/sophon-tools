use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::utils::version::Version;

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

#[derive(Debug)]
pub struct PackageIter<'a, 'b> {
    branches_iter: GameBranchesIter<'a, 'b>,
    preload: bool,
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameBranches {
    pub game_branches: Vec<GameBranchInfo>,
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageInfo {
    pub package_id: String,
    pub branch: String,
    pub password: String,
    pub tag: String,
    pub diff_tags: Vec<String>,
    pub categories: Vec<PackageCategory>,
}

impl PackageInfo {
    pub fn version(&self) -> Option<Version> {
        Version::from_str(&self.tag).ok()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageCategory {
    pub category_id: String,
    pub matching_field: String,
}
