use clap::Args;

use super::{DownloadParameters, GameCommon};

#[derive(Debug, Args)]
/// Update the game from one version to anotehr
pub struct UpdateArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Currently installed version to update from
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
