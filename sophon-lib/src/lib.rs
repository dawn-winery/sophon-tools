use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    num::NonZeroUsize,
    os::unix::fs::PermissionsExt,
    path::Path,
};

pub use error::SophonError;
pub use game_edition::GameEdition;
use md5::{Digest, Md5};
pub use reqwest;

pub mod api;
pub mod error;
pub mod game_edition;
pub mod installer;
pub mod protos;
pub mod updater;
pub mod utils;

const DEFAULT_CHUNK_RETRIES: u8 = 4;

pub fn prettify_bytes(bytes: u64) -> String {
    if bytes > 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
    } else if bytes > 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
    } else if bytes > 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} B", bytes)
    }
}

fn finalize_file(file: &Path, target: &Path, size: u64, hash: &str) -> Result<(), SophonError> {
    if check_file(file, size, hash)? {
        tracing::debug!(
            result = ?file,
            destination = ?target,
            "File hash check passed, copying into final destination"
        );
        ensure_parent(target)?;
        add_user_write_permission_to_file(target)?;
        std::fs::copy(file, target)?;
        Ok(())
    } else {
        Err(SophonError::FileHashMismatch {
            path: file.to_owned(),
            expected: hash.to_owned(),
            got: file_md5_hash_str(file)?,
        })
    }
}

fn ensure_parent(path: impl AsRef<Path>) -> std::io::Result<()> {
    #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
    if let Some(parent) = path.as_ref().parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    Ok(())
}

fn md5_hash_str(data: &[u8]) -> String {
    format!("{:x}", Md5::digest(data))
}

fn bytes_check_md5(data: &[u8], expected_hash: &str) -> bool {
    let computed_hash = md5_hash_str(data);

    expected_hash == computed_hash
}

// MD5 hash calculation without reading the whole file contents into RAM
pub fn file_md5_hash_str(file_path: impl AsRef<Path>) -> std::io::Result<String> {
    let mut file = File::open(&file_path)?;
    let mut md5 = Md5::new();

    std::io::copy(&mut file, &mut md5)?;

    Ok(format!("{:x}", md5.finalize()))
}

pub fn check_file(
    file_path: impl AsRef<Path>,
    expected_size: u64,
    expected_md5: &str,
) -> std::io::Result<bool> {
    let Ok(fs_metadata) = std::fs::metadata(&file_path) else {
        return Ok(false);
    };

    let file_size = fs_metadata.len();

    if file_size != expected_size {
        return Ok(false);
    }

    let file_md5 = file_md5_hash_str(&file_path)?;

    Ok(file_md5 == expected_md5)
}

fn add_user_write_permission_to_file(path: impl AsRef<Path>) -> std::io::Result<()> {
    if !path.as_ref().exists() {
        return Ok(());
    }

    let mut permissions = std::fs::metadata(&path)?.permissions();

    if permissions.readonly() {
        let perm_mode = permissions.mode();
        let user_write_mode = perm_mode | 0o200;

        permissions.set_mode(user_write_mode);

        std::fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

fn file_region_hash_md5(file: &mut File, offset: u64, length: u64) -> std::io::Result<String> {
    file.seek(SeekFrom::Start(offset))?;

    let mut region_reader = file.take(length);
    let mut hasher = Md5::new();

    std::io::copy(&mut region_reader, &mut hasher)?;

    Ok(format!("{:x}", hasher.finalize()))
}

/// Divides thread count for the two pools. Element 0 is for downloading, element 1 is for
/// patching/assembling
fn divide_threads(thread_count: usize) -> Result<(NonZeroUsize, NonZeroUsize), SophonError> {
    let thread_count =
        NonZeroUsize::new(thread_count).ok_or(SophonError::InvalidThreadAmount(thread_count))?;
    if thread_count.get() == 1 {
        tracing::warn!(
            "Thread count set to 1, but at least 2 are required, returning 1 for each pool"
        );
        // SAFETY: 1 is not zero
        Ok(unsafe {
            (
                NonZeroUsize::new_unchecked(1),
                NonZeroUsize::new_unchecked(1),
            )
        })
    } else {
        // division rounds towards zero, leave less threads for patching/assembly
        let last = thread_count.get() / 2;
        let first = thread_count.get() - last;
        Ok((
            NonZeroUsize::new(first).ok_or(SophonError::InvalidThreadAmount(first))?,
            NonZeroUsize::new(last).ok_or(SophonError::InvalidThreadAmount(last))?,
        ))
    }
}
