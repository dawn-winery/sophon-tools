use serde::{Deserialize, Serialize};

/// Cut down game configs version, only keeping the relevant information, used for autodetect stuff
#[derive(Debug, Serialize, Deserialize)]
pub struct GameConfigs {
    pub launch_configs: Vec<GameLaunchConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GameLaunchConfig {
    pub game: super::game_branches::Game,
    pub exe_file_name: String,
    pub default_download_mode: String,
}
