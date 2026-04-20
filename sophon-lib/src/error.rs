// TODO:
// - Cull some variants of SophonError, especially those that are unused
// - Make some better variants describing where the error happened, perhaps steal anyhow's context
//   idea but simpler, especially useful for I/O errors.
// - Cull unused installer/update messages

use std::{error::Error, path::PathBuf};

use thiserror::Error;

use crate::prettify_bytes;

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

    #[error("Invalid thread amount: {0}, must be above 0")]
    InvalidThreadAmount(usize),
}

impl From<fs_extra::error::Error> for SophonError {
    fn from(err: fs_extra::error::Error) -> Self {
        type FsExtraErrorKind = fs_extra::error::ErrorKind;
        type StdIoErrorKind = std::io::ErrorKind;

        let message = err.to_string();

        let kind = match err.kind {
            FsExtraErrorKind::NotFound => StdIoErrorKind::NotFound,
            FsExtraErrorKind::PermissionDenied => StdIoErrorKind::PermissionDenied,
            FsExtraErrorKind::AlreadyExists => StdIoErrorKind::AlreadyExists,
            FsExtraErrorKind::Interrupted => StdIoErrorKind::Interrupted,
            FsExtraErrorKind::InvalidFolder => StdIoErrorKind::NotADirectory,
            FsExtraErrorKind::InvalidFile => StdIoErrorKind::IsADirectory,
            FsExtraErrorKind::InvalidFileName => StdIoErrorKind::InvalidFilename,
            FsExtraErrorKind::InvalidPath => StdIoErrorKind::InvalidInput,
            FsExtraErrorKind::Io(error) => return Self::IoError(error),
            FsExtraErrorKind::StripPrefix(_) => StdIoErrorKind::InvalidInput,
            FsExtraErrorKind::OsString(_) => StdIoErrorKind::InvalidInput,
            FsExtraErrorKind::Other => StdIoErrorKind::Other,
        };

        Self::IoError(std::io::Error::new(kind, message))
    }
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
