use std::{
    fs::{File, OpenOptions},
    io::{Error, Write},
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Once,
};

use interprocess::os::unix::fifo_file::create_fifo;
use rand::{RngExt, distr::Alphanumeric, rng};

use crate::{
    updater::{PatchFnArgs, PatchLocation},
    utils::read_take_region::ReadTakeRegion,
};

const HPATCHZ_BINARY: &[u8] = include_bytes!("../../external/hpatchz/hpatchz");
const HPATCHZ_MD5: &str = env!("HPATCHZ_MD5");

/// Save the hpatchz binary to disk if this is run for the first time. Otherwise, just return the
/// path
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
pub fn patch(args: PatchFnArgs) -> std::io::Result<()> {
    tracing::debug!("Applying hdiff patch");

    let hpatchz = hpatchz_fs_binary()?;

    let patch_file_path = match args.patch {
        PatchLocation::Filesystem(path) => path.clone(),
        #[allow(
            unreachable_code,
            reason = "Intentionally left like this, for potential future implementation when hpatchz can read from named pipes"
        )]
        PatchLocation::Memory(_) | PatchLocation::FilesystemRegion { .. } => {
            return Err(std::io::Error::other(
                "hpatchz cannot use piped files, convert and save as a file",
            ));
            let pipe_path = PathBuf::from(format!(
                "/tmp/patch-{}.pipe",
                rng()
                    .sample_iter(&Alphanumeric)
                    .map(|c| c as char)
                    .take(16)
                    .collect::<String>()
            ));

            create_fifo(&pipe_path, 0o600)
                .inspect_err(|err| tracing::error!("Error creating named pipe: {err}"))?;

            pipe_path
        }
    };

    let child = Command::new(hpatchz)
        .arg("-f")
        .arg(args.src_file.as_os_str())
        .arg(patch_file_path.as_os_str())
        .arg(args.out_file.as_os_str())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::piped())
        .spawn()?;

    // this is a no-op due to conversion before call in updater and return with error earlier
    if matches!(
        args.patch,
        PatchLocation::Memory(_) | PatchLocation::FilesystemRegion { .. }
    ) {
        let mut patch_pipe = OpenOptions::new()
            .write(true)
            .open(&patch_file_path)
            .inspect_err(|err| tracing::error!("Error opening named pipe for writing: {err}"))?;
        match args.patch {
            PatchLocation::Filesystem(_) => {}
            PatchLocation::Memory(data) => {
                patch_pipe.write_all(data)?;
            }
            PatchLocation::FilesystemRegion {
                combined_path,
                offset,
                length,
            } => {
                let combined_file = File::open(combined_path)?;
                let mut region =
                    combined_file.take_region(std::io::SeekFrom::Start(*offset), *length)?;
                std::io::copy(&mut region, &mut patch_pipe)?;
            }
        }
    }

    let output = child.wait_with_output()?;

    if matches!(
        args.patch,
        PatchLocation::Memory(_) | PatchLocation::FilesystemRegion { .. }
    ) {
        let _ = std::fs::remove_file(&patch_file_path);
    }

    if String::from_utf8_lossy(output.stdout.as_slice()).contains("patch ok!") {
        Ok(())
    } else {
        let err = String::from_utf8_lossy(&output.stderr);

        tracing::error!("Failed to apply hdiff patch: {err}");

        Err(Error::other(format!("Failed to apply hdiff patch: {err}")))
    }
}
