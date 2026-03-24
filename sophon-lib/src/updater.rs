use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{Cursor, Seek, SeekFrom},
    num::NonZeroUsize,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Mutex, atomic::AtomicU64},
    time::Duration,
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use reqwest::{blocking::Client, header::RANGE};

use super::{
    DEFAULT_CHUNK_RETRIES, SophonError,
    api::{
        get_patch_manifest,
        schemas::{sophon_diff::SophonDiff, sophon_manifests::DownloadInfo},
    },
    check_file, file_md5_hash_str, finalize_file, prettify_bytes,
    protos::{
        SophonPatchAssetChunk, SophonPatchAssetProperty, SophonPatchProto, SophonUnusedAssetInfo,
    },
    utils::version::Version,
};
use crate::{
    FileCheckCache,
    utils::{read_reporter::ReadReporter, read_take_region::ReadTakeRegion},
};

#[derive(Debug)]
pub enum Update {
    CheckingFreeSpace(PathBuf),

    CheckingFilesStarted,
    DeletingStarted,

    DeletingProgress {
        deleted_files: u64,
        total_unused: u64,
    },

    DeletingFinished,

    /// `(temp path)`
    DownloadingStarted(PathBuf),

    DownloadingProgressBytes {
        downloaded_bytes: u64,
        total_bytes: u64,
    },

    DownloadingFinished,

    PatchingStarted,

    PatchingProgress {
        patched_files: u64,
        total_files: u64,
    },

    PatchingFinished,

    DownloadingError(SophonError),
    PatchingError(String),

    FileHashCheckFailed(PathBuf),
}

#[derive(Debug, Clone)]
struct FilePatchInfo<'a> {
    file_manifest: &'a SophonPatchAssetProperty,
    patch_chunk: &'a SophonPatchAssetChunk,
    patch_chunk_download_info: &'a DownloadInfo,
    retries_left: u8,
}

impl FilePatchInfo<'_> {
    /// Path to a target file on filesystem
    fn target_file_path(&self, game_dir: impl AsRef<Path>) -> PathBuf {
        game_dir.as_ref().join(&self.file_manifest.asset_name)
    }

    fn orig_file_path(&self, game_dir: impl AsRef<Path>) -> Option<PathBuf> {
        if !self.is_patch() {
            None
        } else {
            Some(game_dir.as_ref().join(&self.patch_chunk.original_file_name))
        }
    }

    /// Path to temporary file to store before patching or as a result of a copy
    /// from patch chunk
    fn tmp_src_filename(&self) -> String {
        format!("{}.tmp", &self.file_manifest.asset_hash_md5)
    }

    /// Path to a temporary file to store patching output to
    fn tmp_out_filename(&self) -> String {
        format!("{}.tmp.out", &self.file_manifest.asset_hash_md5)
    }

    /// Get filename for whatever artifact is needed to patch this file.
    /// it's either an hdiff patch file or a plain blob that needs to be copied
    /// as the entire contents of the new file.
    fn artifact_filename(&self) -> String {
        if self.is_patch() {
            format!(
                "{}-{}.hdiff",
                self.patch_chunk.patch_name, self.file_manifest.asset_hash_md5
            )
        } else {
            format!("{}.bin", self.file_manifest.asset_hash_md5)
        }
    }

    /// Returns true if the file is updated by patching.
    /// Returns false if the file is simply copied from the chunk.
    fn is_patch(&self) -> bool {
        !self.patch_chunk.original_file_name.is_empty()
    }

    /// Value for a Range header for downloading the file
    fn download_range(&self) -> String {
        format!(
            "bytes={}-{}",
            self.patch_chunk.patch_offset,
            self.patch_chunk.patch_offset + self.patch_chunk.patch_length - 1
        )
    }

    fn download_url(&self) -> String {
        self.patch_chunk_download_info
            .download_url(&self.patch_chunk.patch_name)
    }
}

#[derive(Debug)]
struct UpdateIndex<'a> {
    unused: Option<&'a SophonUnusedAssetInfo>,
    unused_deleted: AtomicU64,
    total_bytes: u64,
    downloaded_bytes: AtomicU64,
    files_to_patch: HashMap<&'a String, FilePatchInfo<'a>>,
    file_check_cache: Mutex<FileCheckCache>,
    files_patched: AtomicU64,
}

impl<'a> UpdateIndex<'a> {
    fn new(
        update_manifest: &'a SophonPatchProto,
        patch_chunk_download_info: &'a DownloadInfo,
        from: Version,
    ) -> Self {
        let files_to_patch = update_manifest
            .patch_assets
            .iter()
            .filter_map(|spap| {
                Some((
                    &spap.asset_name,
                    FilePatchInfo {
                        file_manifest: spap,
                        patch_chunk_download_info,
                        patch_chunk: spap
                            .asset_patch_chunks
                            .iter()
                            .find_map(|(fromver, pchunk)| (*fromver == from).then_some(pchunk))?,
                        retries_left: DEFAULT_CHUNK_RETRIES,
                    },
                ))
            })
            .collect::<HashMap<_, _>>();

        // use hashmap to deduplicate the chunks
        let mut patch_chunks_map = HashMap::new();
        for file_info in files_to_patch.values() {
            if !patch_chunks_map.contains_key(&file_info.patch_chunk.patch_name) {
                patch_chunks_map.insert(
                    &file_info.patch_chunk.patch_name,
                    file_info.patch_chunk.patch_size,
                );
            }
        }
        let total_bytes = patch_chunks_map.values().sum();

        Self {
            unused: update_manifest
                .unused_assets
                .iter()
                .find_map(|(fromver, unused)| (*fromver == from).then_some(unused)),
            unused_deleted: AtomicU64::new(0),
            total_bytes,
            downloaded_bytes: AtomicU64::new(0),
            file_check_cache: Mutex::new(FileCheckCache::with_capacity(files_to_patch.len())),
            files_to_patch,
            files_patched: AtomicU64::new(0),
        }
    }

    #[inline]
    fn total_files(&self) -> u64 {
        self.files_to_patch.len() as u64
    }

    #[inline]
    fn total_unused(&self) -> u64 {
        self.unused.map(|una| una.assets.len()).unwrap_or(0) as u64
    }

    #[inline]
    fn msg_bytes(&self) -> Update {
        Update::DownloadingProgressBytes {
            downloaded_bytes: self
                .downloaded_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            total_bytes: self.total_bytes,
        }
    }

    #[inline]
    fn msg_patched(&self) -> Update {
        Update::PatchingProgress {
            patched_files: self
                .files_patched
                .load(std::sync::atomic::Ordering::Acquire),
            total_files: self.total_files(),
        }
    }

    #[inline]
    fn msg_deleted(&self) -> Update {
        Update::DeletingProgress {
            deleted_files: self
                .unused_deleted
                .load(std::sync::atomic::Ordering::Acquire),
            total_unused: self.total_unused(),
        }
    }

    fn add_msg_bytes(&self, amount: u64) -> Update {
        Update::DownloadingProgressBytes {
            downloaded_bytes: self
                .downloaded_bytes
                .fetch_add(amount, std::sync::atomic::Ordering::Relaxed)
                + amount,
            total_bytes: self.total_bytes,
        }
    }

    fn add_msg_patched(&self, amount: u64) -> Update {
        Update::PatchingProgress {
            patched_files: self
                .files_patched
                .fetch_add(amount, std::sync::atomic::Ordering::Relaxed),
            total_files: self.total_files(),
        }
    }

    fn add_msg_deleted(&self, amount: u64) -> Update {
        Update::DeletingProgress {
            deleted_files: self
                .unused_deleted
                .fetch_add(amount, std::sync::atomic::Ordering::Relaxed),
            total_unused: self.total_unused(),
        }
    }

    /// Process chunk download failure. Either pushes the chunk onto the retries
    /// queue or sends the chunk download fail update message using the
    /// updater. Refer to [Self::count_chunk_fail] for more info.
    fn process_download_fail<'b>(
        &self,
        mut file: FilePatchInfo<'a>,
        retries_queue: &'b Mutex<VecDeque<FilePatchInfo<'a>>>,
        updater: impl Fn(Update) + 'b,
    ) {
        if file.retries_left == 0 {
            (updater)(Update::DownloadingError(SophonError::ChunkDownloadFailed(
                file.patch_chunk.patch_name.clone(),
            )))
        } else {
            file.retries_left -= 1;
            let Ok(mut queue_lock) = retries_queue.lock() else {
                tracing::error!("The queue lock has been poisoned");
                return;
            };
            queue_lock.push_back(file);
        }
    }

    fn check_file(&self, file_path: &Path, expected_size: u64, expected_md5: &str) -> bool {
        let mut cache = self.file_check_cache.lock().expect("thread was poisoned");
        cache.check_file(file_path, expected_size, expected_md5)
    }
}

// TODO: in-memory queue, queue limit

#[derive(Debug, Clone)]
pub enum PatchLocation {
    Memory(Bytes),
    Filesystem(PathBuf),
    FilesystemRegion {
        combined_path: PathBuf,
        offset: u64,
        length: u64,
    },
}

impl PatchLocation {
    fn size(&self) -> std::io::Result<u64> {
        match self {
            Self::Filesystem(path) => Ok(std::fs::metadata(path)?.size()),
            Self::Memory(buf) => Ok(buf.len() as u64),
            Self::FilesystemRegion { length, .. } => Ok(*length),
        }
    }

    fn cleanup(&self) {
        if let Self::Filesystem(path) = self {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Convert [`Self::Memory`] and [`Self::FilesystemRegion`] to [`Self::Filesystem`]
    /// In case of `self` already being [`Self::Filesystem`], no-op, does not copy or move the file
    fn as_single_file(&self, save_location: PathBuf) -> std::io::Result<Self> {
        match self {
            Self::Filesystem(_) => Ok(self.clone()),
            Self::Memory(data) => {
                std::fs::write(&save_location, data)?;
                Ok(Self::Filesystem(save_location))
            }
            Self::FilesystemRegion {
                combined_path,
                offset,
                length,
            } => {
                let mut src_region =
                    File::open(combined_path)?.take_region(SeekFrom::Start(*offset), *length)?;
                let mut out_file = File::create(&save_location)?;
                std::io::copy(&mut src_region, &mut out_file)?;
                Ok(Self::Filesystem(save_location))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PatchFnArgs<'a> {
    pub patch: &'a PatchLocation,
    pub src_file: &'a Path,
    pub out_file: &'a Path,
}

type BoxPatchFn = Box<dyn Fn(PatchFnArgs<'_>) -> std::io::Result<()> + Sync>;

pub struct SophonPatcher {
    pub client: Client,
    pub patch_manifest: SophonPatchProto,
    pub diff_info: SophonDiff,
    pub temp_folder: PathBuf,
    pub patch_function: Option<BoxPatchFn>,
    pub last_file_suffix: Option<String>,
    pub check_free_space: bool,
    pub patches_in_memory: bool,
    pub patch_queue_mem_limit: Option<u64>,
}

impl std::fmt::Debug for SophonPatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SophonPatcher")
            .field("client", &self.client)
            .field("patch_manifest", &self.patch_manifest)
            .field("diff_info", &self.diff_info)
            .field("check_free_space", &self.check_free_space)
            .field("temp_folder", &self.temp_folder)
            .field("last_file_suffix", &self.last_file_suffix)
            .finish()
    }
}

impl SophonPatcher {
    pub fn new(
        client: Client,
        diff: &SophonDiff,
        temp_dir: impl AsRef<Path>,
        patch_function: Option<BoxPatchFn>,
    ) -> Result<Self, SophonError> {
        #[cfg(not(any(feature = "vendored-hpatchz", feature = "paimon")))]
        let patch_function = Some(patch_function.expect(
            "Hpatchz or rust hdiff parser not included with the crate, custom function required but was not provided",
        ));
        Ok(Self {
            patch_manifest: get_patch_manifest(&client, diff)?,
            client,
            diff_info: diff.clone(),
            check_free_space: true,
            temp_folder: temp_dir.as_ref().to_owned(),
            patch_function,
            last_file_suffix: Some("globalgamemanagers".to_owned()),
            patches_in_memory: false,
            patch_queue_mem_limit: None,
        })
    }

    #[inline]
    pub fn with_free_space_check(mut self, check: bool) -> Self {
        self.check_free_space = check;

        self
    }

    #[inline]
    pub fn with_temp_folder(mut self, temp_folder: impl Into<PathBuf>) -> Self {
        self.temp_folder = temp_folder.into();

        self
    }

    pub fn update(
        &self,
        target_dir: impl AsRef<Path>,
        from: Version,
        thread_count: usize,
        updater: impl Fn(Update) + Clone + Send,
    ) -> Result<(), SophonError> {
        let (download_threads, patch_threads) = super::divide_threads(thread_count)?;

        if self.check_free_space && !self.patches_in_memory {
            tracing::info!("Checking free space availability");
            (updater)(Update::CheckingFreeSpace(self.temp_folder.clone()));

            // TODO: queue limit check when the queue limit is implemented

            let download_bytes: u64 = self
                .diff_info
                .stats
                .get(&from.to_string())
                .unwrap()
                .compressed_size
                .parse()
                .unwrap();

            let already_downloaded_size = fs_extra::dir::get_size(&self.temp_folder)?;

            let size_to_check = download_bytes.saturating_sub(already_downloaded_size);

            if size_to_check > 0 {
                Self::free_space_check(updater.clone(), &self.temp_folder, size_to_check)?;
            }
        }

        self.create_temp_dirs()?;

        self.update_multithreaded(
            download_threads,
            patch_threads,
            target_dir,
            from,
            updater.clone(),
        );

        Ok(())
    }

    fn update_multithreaded(
        &self,
        download_threads: NonZeroUsize,
        patch_threads: NonZeroUsize,
        game_folder: impl AsRef<Path>,
        from: Version,
        updater: impl Fn(Update) + Clone + Send,
    ) {
        let update_index =
            UpdateIndex::new(&self.patch_manifest, &self.diff_info.diff_download, from);

        tracing::info!(
            total_bytes = prettify_bytes(update_index.total_bytes),
            total_files = update_index.total_files(),
            delete_files = update_index.total_unused(),
            "Starting multi-thread updater"
        );

        (updater)(update_index.msg_deleted());
        (updater)(update_index.msg_patched());
        (updater)(update_index.msg_bytes());

        let (file_patch_sender, file_patch_receiver) = crossbeam_channel::unbounded();

        let game_folder = game_folder.as_ref();

        (updater)(Update::CheckingFilesStarted);

        // Filter out:
        // - files which are already updated
        // - files whose source file is invalid or does not exist
        let download_queue = Mutex::new(VecDeque::from_iter(
            update_index
                .files_to_patch
                .values()
                .filter(|patch_info| {
                    if check_file(
                        patch_info.target_file_path(game_folder),
                        patch_info.file_manifest.asset_size,
                        &patch_info.file_manifest.asset_hash_md5,
                    )
                    .unwrap_or(false)
                    {
                        #[cfg(feature = "extra-logs")]
                        tracing::debug!(
                            filename = patch_info.file_manifest.asset_name,
                            "File is already patched, skipping",
                        );
                        (updater)(update_index.add_msg_bytes(patch_info.patch_chunk.patch_length));
                        (updater)(update_index.add_msg_patched(1));
                        return false;
                    } else if let Some(orig_file_path) = patch_info.orig_file_path(game_folder) {
                        #[allow(clippy::collapsible_if)]
                        if !check_file(
                            &orig_file_path,
                            patch_info.patch_chunk.original_file_length,
                            &patch_info.patch_chunk.original_file_md5,
                        )
                        .unwrap_or(false)
                        {
                            tracing::warn!(
                                filename = patch_info.patch_chunk.original_file_name,
                                expected_md5 = patch_info.patch_chunk.original_file_md5,
                                expected_length = patch_info.patch_chunk.original_file_length,
                                "The source file is invalid or does not exist, skipping",
                            );

                            let err = SophonError::FileHashMismatch {
                                path: orig_file_path.clone(),
                                expected: patch_info.patch_chunk.original_file_md5.clone(),
                                got: file_md5_hash_str(orig_file_path)
                                    .unwrap_or_else(|_| "<could not generate hash>".to_owned()),
                            };
                            (updater)(Update::DownloadingError(err));

                            return false;
                        }
                    }
                    true
                })
                .cloned(),
        ));

        tracing::debug!("Spawning worker threads");

        // Same as download/install, but the deleted files are going to be
        // deleted in the main thread.
        std::thread::scope(|scope| {
            let index_ref = &update_index;
            let download_queue_ref = &download_queue;

            (updater)(Update::DownloadingStarted(game_folder.to_owned()));

            for i in 0..download_threads.get() {
                let sender_clone = file_patch_sender.clone();
                let updater_clone = updater.clone();
                scope.spawn(move || {
                    let _span = tracing::debug_span!("Download thread", thread_idx = i).entered();

                    self.artifact_download_loop(
                        download_queue_ref,
                        Some(sender_clone),
                        index_ref,
                        updater_clone,
                    );
                });
            }

            (updater)(Update::PatchingStarted);

            // Patching threads
            for i in 0..patch_threads.get() {
                let updater_clone = updater.clone();
                let receiver_clone = file_patch_receiver.clone();

                scope.spawn(move || {
                    let _span = tracing::debug_span!("Patching thread", thread_idx = i).entered();

                    self.file_patch_loop(game_folder, updater_clone, index_ref, receiver_clone);
                });
            }

            // Unused file deletion - in main thread
            if let Some(unused) = &update_index.unused {
                (updater)(Update::DeletingStarted);

                let _deleting_unused_span =
                    tracing::debug_span!("Deleting unused", amount = unused.assets.len()).entered();

                // Deleting unused files
                for unused_asset in &unused.assets {
                    // Ignore any I/O errors (e.g. missing files, etc)
                    let _ = std::fs::remove_file(game_folder.join(&unused_asset.file_name));

                    (updater)(update_index.add_msg_deleted(1));
                }

                (updater)(Update::DeletingFinished);
            }

            // Make sure to drop the sender and receiver used to clone handles from
            drop(file_patch_sender);
            drop(file_patch_receiver);
        });

        if let Some(last_file_suffix) = &self.last_file_suffix {
            self.last_file_handler(game_folder, &updater, &update_index, last_file_suffix);
        }

        (updater)(Update::PatchingFinished);
    }

    pub fn pre_download(
        &self,
        from: Version,
        thread_count: usize,
        updater: impl Fn(Update) + Clone + Send,
    ) -> Result<(), SophonError> {
        if self.check_free_space {
            tracing::info!("Checking free space availability");
            (updater)(Update::CheckingFreeSpace(self.temp_folder.clone()));

            let download_bytes = self
                .diff_info
                .stats
                .get(&from.to_string())
                .unwrap()
                .compressed_size
                .parse()
                .unwrap();

            Self::free_space_check(updater.clone(), &self.temp_folder, download_bytes)?;
        }

        self.create_temp_dirs()?;

        self.predownload_multithreaded(thread_count, from, updater.clone());

        let marker_file_path = self.files_temp().join(".predownloadcomplete");
        File::create(marker_file_path)?;

        Ok(())
    }

    fn predownload_multithreaded(
        &self,
        _thread_count: usize,
        from: Version,
        updater: impl Fn(Update) + Clone + Send,
    ) {
        tracing::debug!("Starting multithreaded update predownload process");

        let update_index =
            UpdateIndex::new(&self.patch_manifest, &self.diff_info.diff_download, from);

        tracing::info!(
            "{} files to download, {} download total",
            update_index.files_to_patch.len(),
            prettify_bytes(update_index.total_bytes)
        );

        (updater)(update_index.msg_bytes());

        let mut dedupe_set = HashSet::new();

        let download_queue = Mutex::new(VecDeque::from_iter(
            update_index
                .files_to_patch
                .values()
                .filter(|file| dedupe_set.insert(&file.patch_chunk.patch_name))
                .cloned(),
        ));

        tracing::debug!("Starting download");

        std::thread::scope(|scope| {
            let updater_clone = updater.clone();

            scope.spawn(|| {
                let _span = tracing::trace_span!("Download thread").entered();

                (updater_clone)(Update::DownloadingStarted(self.temp_folder.clone()));

                self.artifact_download_loop(&download_queue, None, &update_index, updater_clone);
            });
        });

        (updater)(Update::DownloadingFinished);
    }

    /// Loops over the tasks and retries and tries to download them, pushing
    /// onto the patch queue if the download succeedes. If both the tasks
    /// iterator and the retries queues return nothing, checks if they are empty
    /// and then checks if there are any unfinished patches and waits for either
    /// all patches to finish applying or a new retry being pushed onto the
    /// queue.
    fn artifact_download_loop<'a, 'b>(
        &self,
        task_queue: &'b Mutex<VecDeque<FilePatchInfo<'a>>>,
        patch_queue: Option<Sender<(PatchLocation, FilePatchInfo<'a>)>>,
        update_index: &'b UpdateIndex<'a>,
        updater: impl Fn(Update) + 'b,
    ) {
        while let Some(task) = {
            let Ok(mut queue_lock) = task_queue.lock() else {
                tracing::error!("The queue lock has been poisoned");
                return;
            };
            let val = queue_lock.pop_front();
            drop(queue_lock);
            val
        } {
            // Check if the file already exists on disk and if it does,
            // skip re-downloading it
            let artifact_path = self.tmp_artifact_file_path(&task);
            let combined_file_path = self.tmp_patch_blob_path(task.patch_chunk);

            let download_res = if patch_queue.is_none() {
                // preload
                if update_index.check_file(
                    &combined_file_path,
                    task.patch_chunk.patch_size,
                    &task.patch_chunk.patch_md5,
                ) {
                    (updater)(update_index.add_msg_bytes(task.patch_chunk.patch_length));
                    Ok(())
                } else {
                    self.download_patch_blob(
                        task.patch_chunk,
                        task.patch_chunk_download_info,
                        update_index,
                        &updater,
                    )
                }
                .map(|_| PatchLocation::FilesystemRegion {
                    combined_path: combined_file_path,
                    offset: task.patch_chunk.patch_offset,
                    length: task.patch_chunk.patch_length,
                })
            } else if update_index.check_file(
                &combined_file_path,
                task.patch_chunk.patch_size,
                &task.patch_chunk.patch_md5,
            ) {
                //self.get_patch_from_combined(&task)
                Ok(PatchLocation::FilesystemRegion {
                    combined_path: combined_file_path,
                    offset: task.patch_chunk.patch_offset,
                    length: task.patch_chunk.patch_length,
                })
            } else if artifact_path.exists() {
                #[cfg(feature = "extra-logs")]
                tracing::debug!(
                    artifact = ?artifact_path,
                    "Artifact already exists, skipping download"
                );

                Ok(PatchLocation::Filesystem(artifact_path.clone()))
            } else {
                self.download_patch_range(&task)
            };

            match download_res {
                Ok(loc) => {
                    #[allow(clippy::collapsible_if)]
                    if let Some(patch_queue) = &patch_queue {
                        // udpating downlaod counter is handled in combined-file downloader
                        (updater)(update_index.add_msg_bytes(task.patch_chunk.patch_length));
                        if let Err(err) =
                            patch_queue.send_timeout((loc, task), Duration::from_secs(10))
                        {
                            match err {
                                crossbeam_channel::SendTimeoutError::Disconnected(_) => {
                                    tracing::error!(
                                        "Patching threads disconnected before downloading is done"
                                    );
                                    return;
                                }
                                crossbeam_channel::SendTimeoutError::Timeout(val) => {
                                    tracing::error!(
                                        "Downloaded task send timeout, pushing the task back onto download queue"
                                    );
                                    let Ok(mut queue_lock) = task_queue.lock() else {
                                        tracing::error!("The queue lock has been poisoned");
                                        return;
                                    };
                                    queue_lock.push_back(val.1);
                                }
                            }
                        }
                    }
                }

                Err(err) => {
                    tracing::error!(
                        patch_name = task.patch_chunk.patch_name,
                        ?err,
                        "Failed to download patch",
                    );

                    let _ = std::fs::remove_file(&artifact_path);

                    (updater)(Update::DownloadingError(err));

                    update_index.process_download_fail(task, task_queue, &updater);
                }
            }
        }
    }

    #[tracing::instrument(
        level = "trace", ret, skip_all,
        fields(
            patch_chunk = chunk_info.patch_name,
            url = download_info.download_url(&chunk_info.patch_name),
        )
    )]
    fn download_patch_blob(
        &self,
        chunk_info: &SophonPatchAssetChunk,
        download_info: &DownloadInfo,
        update_index: &UpdateIndex<'_>,
        updater: impl Fn(Update),
    ) -> Result<(), SophonError> {
        let download_url = download_info.download_url(&chunk_info.patch_name);
        let resp = self.client.get(download_url).send()?.error_for_status()?;

        #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
        if let Some(length) = resp.content_length() {
            if length != chunk_info.patch_size {
                return Err(SophonError::DownloadSizeMismatch {
                    name: "Content Length",
                    expected: chunk_info.patch_size,
                    got: length,
                });
            }
        }

        let mut reader = ReadReporter::new(resp, |added| {
            (updater)(update_index.add_msg_bytes(added));
        });

        let out_filename = self
            .patch_chunk_temp_folder()
            .join(format!("{}.bin", chunk_info.patch_name));
        let mut out_file = File::create(&out_filename)?;

        std::io::copy(&mut reader, &mut out_file)?;

        drop(reader);
        drop(out_file);

        if !check_file(&out_filename, chunk_info.patch_size, &chunk_info.patch_md5).unwrap_or(false)
        {
            return Err(SophonError::ChunkHashMismatch {
                expected: chunk_info.patch_md5.clone(),
                got: file_md5_hash_str(&out_filename)?,
            });
        }

        Ok(())
    }

    // instrumenting to maybe try and see how much time it takes to download, hash
    // check, and apply
    #[tracing::instrument(
        level = "trace", ret, skip_all,
        fields(
            file = task.file_manifest.asset_name,
            patch_chunk = task.patch_chunk.patch_name,
            url = task.download_url(),
            range = task.download_range()
        )
    )]
    fn download_patch_range(&self, task: &FilePatchInfo) -> Result<PatchLocation, SophonError> {
        let download_url = task.download_url();
        let download_range_val = task.download_range();
        let out_filename = self.tmp_artifact_file_path(task);

        let resp = self
            .client
            .get(download_url)
            .header(RANGE, download_range_val)
            .send()?
            .error_for_status()?;

        // Don't have a hash for the patch, can't check it here, check the hash
        // before using (or just check the resulting file, copy-over will hash
        // mismatch, patching will likely just fail, less likely succeed and
        // produce wrong file)
        #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
        if let Some(length) = resp.content_length() {
            if length != task.patch_chunk.patch_length {
                return Err(SophonError::DownloadSizeMismatch {
                    name: "Content Length",
                    expected: task.patch_chunk.patch_length,
                    got: length,
                });
            }
        }

        let body = resp.bytes()?;

        if body.len() as u64 != task.patch_chunk.patch_length {
            return Err(SophonError::DownloadSizeMismatch {
                name: "Response body",
                expected: task.patch_chunk.patch_length,
                got: body.len() as u64,
            });
        }

        // PatchMd5 is a hash for the combined blob, not the chunk this function downloads
        /*
        if !bytes_check_md5(&body, &task.patch_chunk.PatchMd5) {
            return Err(SophonError::ChunkHashMismatch {
                expected: task.patch_chunk.PatchMd5.clone(),
                got: md5_hash_str(&body),
            });
        }
        */

        if self.patches_in_memory {
            Ok(PatchLocation::Memory(body))
        } else {
            std::fs::write(&out_filename, body)?;
            Ok(PatchLocation::Filesystem(out_filename))
        }
    }

    fn file_patch_loop<'a, 'b>(
        &self,
        game_folder: &'b Path,
        updater: impl Fn(Update) + 'b,
        update_index: &'b UpdateIndex<'a>,
        queue: Receiver<(PatchLocation, FilePatchInfo<'a>)>,
    ) {
        while let Ok((loc, task)) = queue.recv() {
            self.file_patch_handler(loc, &task, update_index, game_folder, &updater);
        }
    }

    fn last_file_handler(
        &self,
        game_folder: &Path,
        updater: impl Fn(Update),
        update_index: &UpdateIndex<'_>,
        last_file_suffix: &str,
    ) {
        let last_file_path = self.files_temp().join("last_file.tmp");
        if last_file_path.exists() {
            // todo: global OnceLock/Mutex<Vec<FileInfo>> for last file(s) rather than this
            // single-file mess
            let last_file_task = update_index
                .files_to_patch
                .values()
                .find(|task| task.file_manifest.asset_name.ends_with(last_file_suffix))
                .expect("The file was encountered during download, it must exist in the index");
            let target_path = last_file_task.target_file_path(game_folder);
            if let Err(err) = finalize_file(
                &last_file_path,
                &target_path,
                last_file_task.file_manifest.asset_size,
                &last_file_task.file_manifest.asset_hash_md5,
            ) {
                tracing::error!(?err, "Failed to install last file");
                (updater)(Update::DownloadingError(err))
            }

            let _ = std::fs::remove_file(&last_file_path);
        }
    }

    fn file_patch_handler<'a, 'b>(
        &self,
        patch_loc: PatchLocation,
        file_patch_task: &'a FilePatchInfo<'a>,
        update_index: &'b UpdateIndex<'a>,
        game_folder: &'b Path,
        updater: impl Fn(Update) + 'b,
    ) {
        let res = {
            let target_path = file_patch_task.target_file_path(game_folder);

            if let Ok(true) = check_file(
                &target_path,
                file_patch_task.file_manifest.asset_size,
                &file_patch_task.file_manifest.asset_hash_md5,
            ) {
                // shouldn't be encountered since the download queue is filtered, but keeping the
                // check and message just in case
                tracing::debug!(
                    file = ?target_path,
                    "File appears to be already patched"
                );

                Ok(())
            } else if let Some(orig_file_path) = file_patch_task.orig_file_path(game_folder) {
                self.file_patch(&orig_file_path, patch_loc, file_patch_task, game_folder)
            } else {
                self.file_copy_over(patch_loc, file_patch_task, game_folder)
            }
        };

        match res {
            Ok(()) => {
                #[cfg(feature = "extra-logs")]
                tracing::debug!(
                    name = ?file_patch_task.file_manifest.asset_name,
                    "Successfully patched"
                );

                (updater)(update_index.add_msg_patched(1));
            }

            Err(e) => {
                tracing::error!(
                    error = ?e,
                    file = file_patch_task.file_manifest.asset_name,
                    "Patching failed"
                );

                (updater)(Update::PatchingError(e.to_string()));

                self.cleanup_on_fail(file_patch_task);
            }
        }
    }

    fn file_copy_over(
        &self,
        patch_loc: PatchLocation,
        file_patch_task: &FilePatchInfo,
        game_folder: &Path,
    ) -> Result<(), SophonError> {
        let target_path = file_patch_task.target_file_path(game_folder);

        // isn't this double-checking? Checked during queue building iirc.
        /*
        if let Ok(true) = check_file(
            &target_path,
            file_patch_task.file_manifest.asset_size,
            &file_patch_task.file_manifest.asset_hash_md5,
        ) {
            tracing::debug!(file = ?target_path, "File appears to be already patched, marking as success");

            return Ok(());
        }
        */

        let tmp_file_path = self.tmp_out_file_path(file_patch_task);

        // this is less of a mess but still a bit messy
        let extracted_file = match patch_loc {
            PatchLocation::Filesystem(patch_path) => {
                let mut patch_file = File::open(&patch_path)?;

                super::utils::hdiff_newfile::try_new_file_hdiff(
                    patch_file.metadata().map(|m| m.size())?,
                    &mut patch_file,
                    &tmp_file_path,
                )?
                .map(|_| tmp_file_path)
                .unwrap_or(patch_path)
            }
            PatchLocation::FilesystemRegion {
                combined_path,
                offset,
                length,
            } => {
                let combo_file = File::open(&combined_path)?;
                let mut region = (&combo_file).take_region(SeekFrom::Start(offset), length)?;

                super::utils::hdiff_newfile::try_new_file_hdiff(
                    length,
                    &mut region,
                    &tmp_file_path,
                )?
                .map(Result::<_, std::io::Error>::Ok)
                .unwrap_or_else(|| {
                    region.seek(SeekFrom::Start(0))?;
                    let mut out_file = File::create(&tmp_file_path)?;
                    std::io::copy(&mut region, &mut out_file)?;
                    Ok(())
                })?;
                tmp_file_path
            }
            PatchLocation::Memory(data) => {
                let mut data_cursor = Cursor::new(data);

                super::utils::hdiff_newfile::try_new_file_hdiff(
                    data_cursor.get_ref().len() as u64,
                    &mut data_cursor,
                    &tmp_file_path,
                )?
                .map(Result::Ok)
                .unwrap_or_else(|| std::fs::write(&tmp_file_path, data_cursor.get_ref()))?;
                tmp_file_path
            }
        };

        finalize_file(
            &extracted_file,
            &target_path,
            file_patch_task.file_manifest.asset_size,
            &file_patch_task.file_manifest.asset_hash_md5,
        )
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                asset_name = file_patch_task.file_manifest.asset_name,
                "Error with new file"
            );
            tracing::debug!(?file_patch_task, "Errored file task information");
        })?;

        let _ = std::fs::remove_file(&extracted_file);

        Ok(())
    }

    fn file_patch(
        &self,
        orig_file_path: &Path,
        patch_loc: PatchLocation,
        file_patch_task: &FilePatchInfo,
        game_folder: &Path,
    ) -> Result<(), SophonError> {
        /*
        if !check_file(
            orig_file_path,
            file_patch_task.patch_chunk.original_file_length,
            &file_patch_task.patch_chunk.original_file_md5,
        )? {
            // A better way would be to mark the download as failed right away
            // instead of having this repeat all the retries. But it's easier to
            // handle this rare faulty edge case this way.
            tracing::error!(file = ?orig_file_path, "Original file doesn't pass hash check, cannot patch file");

            return Err(SophonError::FileHashMismatch {
                path: orig_file_path.to_owned(),
                expected: file_patch_task.patch_chunk.original_file_md5.clone(),
                got: file_md5_hash_str(orig_file_path)?,
            });
        }
        */

        let tmp_src_path = self.tmp_src_file_path(file_patch_task);
        let tmp_out_path = self.tmp_out_file_path(file_patch_task);
        //let artifact = self.tmp_artifact_file_path(file_patch_task);

        std::fs::copy(orig_file_path, &tmp_src_path)?;

        self.patch(PatchFnArgs {
            patch: &patch_loc,
            src_file: &tmp_src_path,
            out_file: &tmp_out_path,
        })?;

        patch_loc.cleanup();

        let target = if self
            .last_file_suffix
            .as_ref()
            .map(|suffix| file_patch_task.file_manifest.asset_name.ends_with(suffix))
            .unwrap_or(false)
        {
            self.files_temp().join("last_file.tmp")
        } else {
            file_patch_task.target_file_path(game_folder)
        };

        finalize_file(
            &tmp_out_path,
            &target,
            file_patch_task.file_manifest.asset_size,
            &file_patch_task.file_manifest.asset_hash_md5,
        )?;

        // Clean up a bit after patching
        let _ = std::fs::remove_file(&tmp_src_path);
        let _ = std::fs::remove_file(&tmp_out_path);

        Ok(())
    }

    /// Remove all the files that might have been created. Temporary files,
    /// downloads, etc to prepare for a clean re-downlaod of the artifact and
    /// attempting to patch again
    fn cleanup_on_fail(&self, file_info: &FilePatchInfo) {
        let tmp_src = self.tmp_src_file_path(file_info);
        let tmp_out = self.tmp_out_file_path(file_info);
        let artifact = self.tmp_artifact_file_path(file_info);
        for path in [tmp_src, tmp_out, artifact] {
            // Ignore errors (missing file, permissions, etc)
            let _ = std::fs::remove_file(path);
        }
    }

    /// Folder to temporarily store files being updated (patched, created, etc).
    #[inline]
    pub fn files_temp(&self) -> PathBuf {
        self.temp_folder
            .join(format!("updating-{}", self.diff_info.matching_field))
    }

    fn tmp_src_file_path(&self, file_info: &FilePatchInfo) -> PathBuf {
        self.files_temp().join(file_info.tmp_src_filename())
    }

    fn tmp_out_file_path(&self, file_info: &FilePatchInfo) -> PathBuf {
        self.files_temp().join(file_info.tmp_out_filename())
    }

    /// Folder to temporarily store hdiff files
    #[inline]
    fn patches_temp(&self) -> PathBuf {
        self.files_temp().join("patches")
    }

    fn tmp_artifact_file_path(&self, file_info: &FilePatchInfo) -> PathBuf {
        self.patches_temp().join(file_info.artifact_filename())
    }

    fn tmp_patch_blob_path(&self, patch_info: &SophonPatchAssetChunk) -> PathBuf {
        self.patch_chunk_temp_folder()
            .join(format!("{}.bin", patch_info.patch_name))
    }

    /// Folder to temporarily store downloaded patch chunks
    #[inline]
    fn patch_chunk_temp_folder(&self) -> PathBuf {
        self.files_temp().join("patch_chunks")
    }

    /// Create all needed sub-directories in the temp folder
    fn create_temp_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.files_temp())?;
        std::fs::create_dir_all(self.patches_temp())?;
        std::fs::create_dir_all(self.patch_chunk_temp_folder())?;

        Ok(())
    }

    fn free_space_check(
        updater: impl Fn(Update) + Clone + Send,
        path: impl AsRef<Path>,
        required: u64,
    ) -> Result<(), SophonError> {
        (updater)(Update::CheckingFreeSpace(path.as_ref().to_owned()));

        match fs2::available_space(&path) {
            Ok(space) if space >= required => Ok(()),

            Ok(space) => {
                let err = SophonError::NoSpaceAvailable {
                    path: path.as_ref().to_owned(),
                    required,
                    available: space,
                };

                Err(err)
            }

            Err(ioerr) => {
                let err = if ioerr.kind() == std::io::ErrorKind::NotFound {
                    SophonError::PathNotMounted(path.as_ref().to_owned())
                } else {
                    ioerr.into()
                };

                Err(err)
            }
        }
    }

    #[allow(unreachable_code)]
    fn patch(&self, patch_args: PatchFnArgs<'_>) -> std::io::Result<()> {
        if let Some(pfunc) = &self.patch_function {
            return (pfunc)(patch_args);
        }
        #[cfg(feature = "paimon")]
        return super::utils::paimon::paimon_patch(patch_args);
        #[cfg(feature = "vendored-hpatchz")]
        return self.hpatchz_patch(patch_args);
        // Unreachable because:
        // 1. `None` with `not(feature = "vendored-hpatchz")` is caught during struct init
        // 2. `feature = "vendored-hpatchz"` provides a default
        //
        // Still leaving a message if this is somehow reached
        // IMO this macro should be unsafe lol, despite technically being safe (compiler hint with
        // panic)
        unreachable!("No patch function available")
    }

    #[cfg(feature = "vendored-hpatchz")]
    fn hpatchz_patch(&self, args: PatchFnArgs) -> std::io::Result<()> {
        use rand::{RngExt, distr::Alphanumeric, rng};

        let loc_cloned = args.patch.as_single_file(self.patches_temp().join(format!(
                "patch-{}.tmp.patch",
                rng()
                    .sample_iter(&Alphanumeric)
                    .map(|c| c as char)
                    .take(16)
                    .collect::<String>()
            )))?;
        super::utils::hpatchz::patch(PatchFnArgs {
            patch: &loc_cloned,
            ..args
        })?;
        loc_cloned.cleanup();
        Ok(())
    }
}
