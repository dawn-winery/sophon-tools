use serde::{Deserialize, Serialize};

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
