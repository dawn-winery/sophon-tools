use serde::{Deserialize, Serialize};

use crate::api::ApiContext;

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

impl ApiContext {
    fn game_configs_url(&self) -> String {
        format!(
            "{}/hyp/hyp-connect/api/getGameConfigs?launcher_id={}",
            self.edition.branches_host(),
            self.edition.launcher_id()
        )
    }

    crate::api::api_methods! {
        game_configs(get, url = game_configs_url()) -> GameConfigs,
        game_configs_raw(get, raw, url = game_configs_url()) -> String
    }
}
