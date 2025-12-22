use std::{
    error::Error,
    fmt::Write,
    fs::File,
    io::{Read, Seek, SeekFrom},
    iter::Peekable,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    str::FromStr,
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use md5::{Digest, Md5};
pub use reqwest;
use thiserror::Error;

pub mod api;
pub mod installer;
pub mod protos;
pub mod repairer;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ArtifactDownloadState {
    // Chunk successfully downloaded
    Downloaded,
    // Download failed, run out of retries
    Failed,
    // Amount of retries left, 0 means last retry is being run
    Downloading(u8),
}

impl Default for ArtifactDownloadState {
    #[inline]
    fn default() -> Self {
        Self::Downloading(DEFAULT_CHUNK_RETRIES)
    }
}

/// Implement conversion traits into this enum for use with the APIs, used for getting launcher id,
/// api host, etc
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum GameEdition {
    Global,
    China,
    GlobalBeta { launcher_id: String },
    ChinaBeta { launcher_id: String },
}

impl GameEdition {
    #[inline]
    pub fn branches_host(&self) -> &str {
        match self {
            Self::Global => concat!("https://", "sg-hy", "p-api.", "h", "oy", "over", "se.com"),
            Self::China => concat!("https://", "hy", "p-api.", "mi", "h", "oyo", ".com"),
            Self::GlobalBeta { .. } => {
                concat!("https://", "sg-hy", "p-api-beta.", "hoy", "overse.com")
            }
            Self::ChinaBeta { .. } => {
                concat!("https://", "hy", "p-api-beta.", "mi", "h", "oyo", ".com")
            }
        }
    }

    #[inline]
    pub fn api_host(&self) -> &str {
        match self {
            Self::Global => concat!("https://", "sg-pu", "blic-api.", "hoy", "over", "se.com"),
            Self::China => concat!("https://", "api-t", "ak", "umi.", "mi", "h", "oyo", ".com"),
            Self::GlobalBeta { .. } => {
                concat!("https://", "sg-be", "ta-api.", "hoy", "over", "se.com")
            }
            Self::ChinaBeta { .. } => {
                concat!("https://", "downloader-api-beta.", "mih", "oyo", ".com")
            }
        }
    }

    #[inline]
    pub fn launcher_id(&self) -> &str {
        match self {
            Self::Global => "VYTpXlbWo8",
            Self::China => "jGHBHlcOq1",
            Self::GlobalBeta { launcher_id } => launcher_id,
            Self::ChinaBeta { launcher_id } => launcher_id,
        }
    }
}

#[derive(Debug, Error)]
#[error("Unknown game edition: {0}")]
pub struct UnknownValue(String);

impl FromStr for GameEdition {
    type Err = UnknownValue;

    /// Case-insensitive parse
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowercased = s.to_lowercase();
        match lowercased.as_str() {
            "global" => Ok(Self::Global),
            "china" => Ok(Self::China),
            "global-beta" => Ok(Self::GlobalBeta {
                launcher_id: "URRJEUzW3X".to_owned(),
            }),
            "china-beta" => Ok(Self::ChinaBeta {
                launcher_id: "GcFHm7rte6".to_owned(),
            }),
            _ if lowercased.starts_with("global-beta-") => Ok(Self::GlobalBeta {
                launcher_id: s[12..].to_owned(),
            }),
            _ if lowercased.starts_with("china-beta-") => Ok(Self::GlobalBeta {
                launcher_id: s[11..].to_owned(),
            }),
            _ => Err(UnknownValue(lowercased)),
        }
    }
}

/// Worker thread queue (patching, assembling files)
///
/// Basically the crossbeam-deque example usage of the Injector, allows having a shared queue with
/// local queues and jobs getting stolen from otehr lcoal queues in case one dries up
struct ThreadQueue<'a, T> {
    global: &'a Injector<T>,
    local: Worker<T>,
    stealers: &'a [Stealer<T>],
}

impl<'a, T> ThreadQueue<'a, T> {
    /// Based on the example from crossbeam deque
    fn next_job(&self) -> Option<T> {
        self.local.pop().or_else(|| {
            std::iter::repeat_with(|| {
                self.global
                    .steal_batch_and_pop(&self.local)
                    .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
            })
            .find(|s| !s.is_retry())
            .and_then(Steal::success)
        })
    }
}

/// Downloading queue, first iterates over the tasks and then tries to get the tasks from the
/// global retries queue
#[derive(Debug)]
struct DownloadQueue<'b, T, I: Iterator<Item = T> + 'b> {
    tasks_iter: Peekable<I>,
    retries_queue: &'b Injector<T>,
}

impl<'b, I, T> DownloadQueue<'b, T, I>
where
    I: Iterator<Item = T> + 'b,
{
    fn is_empty(&mut self) -> bool {
        self.tasks_iter.peek().is_none() && self.retries_queue.is_empty()
    }
}

impl<'b, I, T> Iterator for DownloadQueue<'b, T, I>
where
    I: Iterator<Item = T> + 'b,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.tasks_iter.next().or_else(|| {
            std::iter::repeat_with(|| self.retries_queue.steal())
                .find(|s| !s.is_retry())
                .and_then(Steal::success)
        })
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
fn file_md5_hash_str(file_path: impl AsRef<Path>) -> std::io::Result<String> {
    let mut file = File::open(&file_path)?;
    let mut md5 = Md5::new();

    std::io::copy(&mut file, &mut md5)?;

    Ok(format!("{:x}", md5.finalize()))
}

fn check_file(
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

// TODO:
// - Cull some variants of SophonError, especially those that are unused
// - Make some better variants describing where the error happened, perhaps steal anyhow's context
//   idea but simpler, especially useful for I/O errors.
// - Cull unused installer/update messages

#[derive(Error, Debug)]
pub enum SophonError {
    /// Specified downloading path is not available in system
    ///
    /// `(path)`
    #[error("Path is not mounted: {0:?}")]
    PathNotMounted(PathBuf),

    /// No free space available under specified path
    #[error("No free space available for specified path: {0:?} (requires {}, available {})", prettify_bytes(*.required), prettify_bytes(*.available))]
    NoSpaceAvailable {
        path: PathBuf,
        required: u64,
        available: u64,
    },

    /// Failed to create or open output file
    #[error("Failed to create output file {path:?} caused by {source}", source = OptionDisplay {value: self.source(), default: "<source not specified>"})]
    OutputFileError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to create or open temporary output file
    #[error("Failed to create temporary output file {path:?} caused by {source}", source = OptionDisplay {value: self.source(), default: "<source not specified>"})]
    TempFileError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Couldn't get metadata of existing output file
    ///
    /// This metadata supposed to be used to continue downloading of the file
    #[error("Failed to read metadata of the output file {path:?}: {message}")]
    OutputFileMetadataError { path: PathBuf, message: String },

    /// reqwest error
    #[error("Reqwest error: {0} caused by {source}", source = OptionDisplay {value: self.source(), default: "<source not specified>"})]
    Reqwest(#[from] reqwest::Error),

    #[error("Chunk hash mismatch: expected `{expected}`, got `{got}`")]
    ChunkHashMismatch { expected: String, got: String },

    #[error("File {path:?} hash mismatch: expected `{expected}`, got `{got}`")]
    FileHashMismatch {
        path: PathBuf,
        expected: String,
        got: String,
    },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to download chunk {0}, out of retries")]
    ChunkDownloadFailed(String),

    #[error("Failed to apply hdiff patch: {0}")]
    PatchingError(String),

    #[error(
        "Failed to download chunk/patch, {name} size mismatch. Expected {expected}, got {got}."
    )]
    DownloadSizeMismatch {
        name: &'static str,
        expected: u64,
        got: u64,
    },
}

struct OptionDisplay<T, D> {
    value: Option<T>,
    default: D,
}

impl<T, D> std::fmt::Display for OptionDisplay<T, D>
where
    T: std::fmt::Display,
    D: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(v) => std::fmt::Display::fmt(v, f),
            None => std::fmt::Display::fmt(&self.default, f),
        }
    }
}
