use serde::{Deserialize, Serialize};

use crate::api::ApiContext;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameScanInfo {
    pub game_scan_info: Vec<ScanInfo>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanInfo {
    pub game_id: String,
    pub game_exe_list: Vec<GameExeHash>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameExeHash {
    pub version: String,
    pub md5: String,
}

impl ApiContext {
    fn game_scan_info_url(&self) -> String {
        format!(
            "{}/hyp/hyp-connect/api/getGameScanInfo?launcher_id={}",
            self.edition.branches_host(),
            self.edition.launcher_id()
        )
    }

    crate::api::api_methods! {
        game_scan(get, url = game_scan_info_url()) -> GameScanInfo,
        game_scan_raw(get, raw, url = game_scan_info_url()) -> String
    }
}
