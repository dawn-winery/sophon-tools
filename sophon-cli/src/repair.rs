use clap::Args;

use super::{DownloadParameters, GameCommon};

#[derive(Debug, Args)]
/// Check and repair game files
pub struct RepairArgs {
    #[command(flatten)]
    game: GameCommon,
    /// Omit to use latest
    #[arg(short, long)]
    version: Option<String>,

    /// Don't actually repair, only check and report broken files
    #[arg(short, long)]
    dry_run: bool,

    #[command(flatten)]
    extra: DownloadParameters,
}
