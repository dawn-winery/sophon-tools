use std::{
    io::Error,
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Once,
};

const HPATCHZ_BINARY: &[u8] = include_bytes!("../../external/hpatchz/hpatchz");
const HPATCHZ_MD5: &str = env!("HPATCHZ_MD5");

fn hpatchz_fs_binary() -> std::io::Result<PathBuf> {
    static FLAG: Once = Once::new();

    let path = std::env::temp_dir().join(format!("hpatchz-{}", HPATCHZ_MD5));

    let mut res = Ok(());

    FLAG.call_once(|| res = save_executable_to(&path));

    res.map(|_| path)
}

fn save_executable_to(path: &Path) -> std::io::Result<()> {
    std::fs::write(path, HPATCHZ_BINARY)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o775))
}

/// Try to apply hdiff patch
#[tracing::instrument(level = "debug")]
pub fn patch(file: &Path, patch: &Path, output: &Path) -> std::io::Result<()> {
    tracing::debug!("Applying hdiff patch");

    let hpatchz = hpatchz_fs_binary()?;

    let output = Command::new(hpatchz)
        .arg("-f")
        .arg(file.as_os_str())
        .arg(patch.as_os_str())
        .arg(output.as_os_str())
        .output()?;

    if String::from_utf8_lossy(output.stdout.as_slice()).contains("patch ok!") {
        Ok(())
    } else {
        let err = String::from_utf8_lossy(&output.stderr);

        tracing::error!("Failed to apply hdiff patch: {err}");

        Err(Error::other(format!("Failed to apply hdiff patch: {err}")))
    }
}
