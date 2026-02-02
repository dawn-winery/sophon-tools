use std::path::{Path, PathBuf};

use clap::Args;
use sophon_lib::{
    GameEdition, SophonError,
    api::{get_game_configs, get_game_scan_info},
    reqwest::{self, blocking::Client},
};

use super::{DownloadParameters, GameCommon};

// this is only for global edition rn
fn match_id_to_exe(
    client: &Client,
    game_id: &str,
    edition: &GameEdition,
) -> Result<Option<String>, SophonError> {
    let game_configs = get_game_configs(client, edition)?;
    Ok(game_configs
        .launch_configs
        .into_iter()
        .find_map(|launch_config| {
            (launch_config.game.id == game_id).then_some(launch_config.exe_file_name)
        }))
}

pub fn autodetect_game_ver(
    game_folder: &Path,
    game: &str,
    edition: &GameEdition,
) -> Result<Option<String>, SophonError> {
    let client = reqwest::blocking::Client::new();
    let game_scan_info = get_game_scan_info(&client, edition)?;
    let Some(filtered) = game_scan_info
        .game_scan_info
        .into_iter()
        .find(|scan_info| scan_info.game_id == game)
    else {
        eprintln!("Game id `{game}` not found! Can only use game id, not game codename.");
        return Ok(None);
    };

    let Some(exe_name) = match_id_to_exe(&client, game, edition)? else {
        return Ok(None);
    };
    let exe_path = game_folder.join(exe_name);

    for hash in filtered.game_exe_list {
        if sophon_lib::file_md5_hash_str(&exe_path)
            .map(|generated_hash| generated_hash == hash.md5)
            .unwrap_or(false)
        {
            return Ok(Some(hash.version));
        }
    }

    Ok(None)
}

#[derive(Debug, Args)]
/// Update the game from one version to anotehr
pub struct UpdateArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Currently installed version to update from. Use value of `auto` to autodetect the installed
    /// game version
    #[arg(long)]
    from: String,
    /// Omit to use latest
    #[arg(long)]
    to: Option<String>,

    /// Whether to use the preload (just downloads all patches and blobs into the cache dir)
    #[arg(short, long)]
    preload: bool,

    #[command(flatten)]
    extra: DownloadParameters,
}

impl UpdateArgs {
    pub fn update(
        mut self,
        game_edition: GameEdition,
        cache_dir: PathBuf,
        thread_count: usize,
    ) -> Result<(), String> {
        if self.from == "auto"
            && let Some(auto_ver) =
                autodetect_game_ver(&self.game.game_dir, &self.game.game, &game_edition)
                    .map_err(|e| e.to_string())
                    .inspect_err(|err| {
                        eprintln!("Error autodetecting game version: {err}");
                    })
                    .unwrap_or(None)
        {
            self.from = auto_ver;
        }

        todo!()
    }
}
