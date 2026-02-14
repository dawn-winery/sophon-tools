use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Seek, Write},
    num::NonZeroUsize,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Mutex, atomic::AtomicU64},
    time::Duration,
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, SendTimeoutError, Sender};
use reqwest::blocking::Client;

use super::{
    SophonError,
    api::{
        get_download_manifest,
        schemas::sophon_manifests::{DownloadInfo, SophonDownloadInfo},
    },
    check_file, file_md5_hash_str, finalize_file, md5_hash_str, prettify_bytes,
    protos::{SophonManifestAssetChunk, SophonManifestAssetProperty, SophonManifestProto},
};
use crate::{DEFAULT_CHUNK_RETRIES, divide_threads, ensure_parent, file_region_hash_md5};

#[derive(Debug)]
pub enum Update {
    CheckingFreeSpace(PathBuf),

    CheckingFiles {
        total_files: u64,
    },
    CheckingFilesProgress {
        passed: u64,
        total: u64,
    },

    /// `(temp path)`
    DownloadingStarted {
        location: PathBuf,
        total_bytes: u64,
        total_files: u64,
    },

    DownloadingProgressBytes {
        downloaded_bytes: u64,
        total_bytes: u64,
    },

    DownloadingProgressFiles {
        downloaded_files: u64,
        total_files: u64,
    },

    DownloadingFinished,

    DownloadingError(SophonError),
}

#[derive(Debug)]
struct ChunkInfo<'a> {
    chunk_manifest: &'a SophonManifestAssetChunk,
    download_info: &'a DownloadInfo,
    retries_left: u8,
}

impl ChunkInfo<'_> {
    fn download_url(&self) -> String {
        self.download_info
            .download_url(&self.chunk_manifest.chunk_name)
    }

    /// returns the expected size and md5 hash that will be used to download and
    /// check this chunk
    #[inline(always)]
    fn chunk_file_info(&self) -> (u64, &str) {
        if self.is_compressed() {
            (
                self.chunk_manifest.chunk_size,
                &self.chunk_manifest.chunk_compressed_hash_md5,
            )
        } else {
            (
                self.chunk_manifest.chunk_size_decompressed,
                &self.chunk_manifest.chunk_decompressed_hash_md5,
            )
        }
    }

    fn is_compressed(&self) -> bool {
        self.download_info.compression == 1
    }

    fn ondisk_filename(&self) -> String {
        if self.is_compressed() {
            format!("{}.chunk.zstd", self.chunk_manifest.chunk_name)
        } else {
            format!("{}.chunk", self.chunk_manifest.chunk_name)
        }
    }
}

#[derive(Debug)]
struct FileInfo<'a> {
    file_manifest: &'a SophonManifestAssetProperty,
    download_info: &'a DownloadInfo,
}

impl<'a> FileInfo<'a> {
    /// Path to a target file on filesystem
    fn target_file_path(&self, game_dir: impl AsRef<Path>) -> PathBuf {
        game_dir.as_ref().join(&self.file_manifest.asset_name)
    }

    /// Path to a temporary file to store the in-progress file
    fn tmp_filename(&self) -> String {
        let asset_name_hashed = md5_hash_str(self.file_manifest.asset_name.as_bytes());
        format!(
            "{asset_name_hashed}-{}.tmp",
            self.file_manifest.asset_hash_md5
        )
    }

    fn chunks_iter(&self) -> impl Iterator<Item = ChunkInfo<'a>> {
        self.file_manifest
            .asset_chunks
            .iter()
            .map(|chunk_manifest| ChunkInfo {
                chunk_manifest,
                download_info: self.download_info,
                retries_left: DEFAULT_CHUNK_RETRIES,
            })
    }
}

#[derive(Debug)]
struct DownloadIndex<'a> {
    chunks_used_in: HashMap<&'a String, Vec<&'a String>>,
    files: HashMap<&'a String, FileInfo<'a>>,
    total_bytes: u64,
    downloaded_bytes: AtomicU64,
    downloaded_files: AtomicU64,
}

impl<'a> DownloadIndex<'a> {
    fn new(download_info: &'a SophonDownloadInfo, manifest: &'a SophonManifestProto) -> Self {
        let mut chunks_info = HashMap::new();
        let mut files = HashMap::with_capacity(manifest.assets.len());

        for file_manifest in &manifest.assets {
            for chunk_manifest in &file_manifest.asset_chunks {
                let chunk_files_list = chunks_info
                    .entry(&chunk_manifest.chunk_name)
                    .or_insert_with(|| (vec![], chunk_manifest.chunk_size));
                chunk_files_list.0.push(&file_manifest.asset_name);
            }

            files.insert(
                &file_manifest.asset_name,
                FileInfo {
                    file_manifest,
                    download_info: &download_info.chunk_download,
                },
            );
        }

        Self {
            total_bytes: chunks_info.iter().map(|(_, (_, size))| size).sum(),
            chunks_used_in: chunks_info
                .into_iter()
                .map(|(chunk_id, (files_list, _))| (chunk_id, files_list))
                .collect(),
            files,
            downloaded_bytes: AtomicU64::new(0),
            downloaded_files: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn total_files(&self) -> u64 {
        self.files.len() as u64
    }

    fn add_msg_files(&self, amount: u64) -> Update {
        Update::DownloadingProgressFiles {
            downloaded_files: self
                .downloaded_files
                .fetch_add(amount, std::sync::atomic::Ordering::Relaxed)
                + amount,
            total_files: self.total_files(),
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

    /// Process chunk download failure. Either pushes the chunk onto the retries
    /// queue or sends the chunk download fail update message using the
    /// updater. Refer to [Self::count_chunk_fail] for more info.
    fn process_download_fail<'b>(
        &self,
        mut chunk: ChunkInfo<'a>,
        retries_queue: &'b Mutex<VecDeque<ChunkInfo<'a>>>,
        updater: impl Fn(Update) + 'b,
    ) {
        if chunk.retries_left == 0 {
            (updater)(Update::DownloadingError(SophonError::ChunkDownloadFailed(
                chunk.chunk_manifest.chunk_name.clone(),
            )))
        } else {
            chunk.retries_left -= 1;
            let Ok(mut queue_lock) = retries_queue.lock() else {
                tracing::error!("The queue lock has been poisoned");
                return;
            };
            queue_lock.push_back(chunk);
        }
    }
}

#[derive(Debug)]
enum ChunkLocation {
    Memory(Bytes),
    Filesystem(PathBuf),
}

impl ChunkLocation {
    fn check(&self, exp_size: u64, exp_hash: &str) -> std::io::Result<bool> {
        #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
        if let Self::Filesystem(path) = self {
            if !path.exists() {
                return Ok(false);
            }
        }
        if self.size()? != exp_size {
            return Ok(false);
        }
        if self.hash()? != exp_hash {
            return Ok(false);
        }
        Ok(true)
    }

    fn size(&self) -> std::io::Result<u64> {
        match self {
            Self::Filesystem(path) => Ok(std::fs::metadata(path)?.size()),
            Self::Memory(buf) => Ok(buf.len() as u64),
        }
    }

    fn hash(&self) -> std::io::Result<String> {
        match self {
            Self::Filesystem(path) => file_md5_hash_str(path),
            Self::Memory(buf) => Ok(md5_hash_str(buf)),
        }
    }

    fn cleanup(&self) {
        if let Self::Filesystem(path) = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Custom [Sender] wrapper that tracks how big the total size of enqueued chunks is
#[derive(Debug, Clone)]
struct ChunkQueueSender<'a, 'b> {
    sender: Sender<(ChunkLocation, ChunkInfo<'a>)>,
    memory_limit: Option<(u64, &'b AtomicU64)>,
}

impl<'a> ChunkQueueSender<'a, '_> {
    /// The timeout is only used for the [Sender::send_timeout] call, ignored while waiting for
    /// "space" in queue
    fn send_timeout(
        &self,
        chunk: (ChunkLocation, ChunkInfo<'a>),
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<(ChunkLocation, ChunkInfo<'a>)>> {
        let chunk_size = if self.memory_limit.is_some() {
            Some(chunk.0.size().unwrap_or_else(|_| {
                // fails only in case of filesystem-backed chunk. Just try to read from chunk
                // info.
                chunk.1.chunk_file_info().0
            }))
        } else {
            None
        };
        while !self.has_space(&chunk.0, &chunk.1) {
            std::thread::sleep(Duration::from_millis(50));
        }
        self.sender.send_timeout(chunk, timeout)?;
        if let Some((_, counter)) = self.memory_limit {
            counter.fetch_add(
                chunk_size.expect("self.memory_limit is Some, as checked earlier"),
                std::sync::atomic::Ordering::Release,
            );
        }
        Ok(())
    }

    fn has_space(&self, loc: &ChunkLocation, chunk_info: &ChunkInfo<'a>) -> bool {
        // Prevent the queue choking on a chunk that is bigger than the limit.
        if self.sender.is_empty() {
            return true;
        }
        let Some((limit, counter)) = self.memory_limit else {
            return true;
        };
        let chunk_size = loc.size().unwrap_or_else(|_| {
            // fails only in case of filesystem-backed chunk. Just try to read from chunk
            // info.
            chunk_info.chunk_file_info().0
        });
        let queue_size = counter.load(std::sync::atomic::Ordering::Acquire);
        queue_size + chunk_size <= limit
    }
}

/// Custom [Receiver] wrapper that tracks how big the total size of enqueued chunks is
#[derive(Debug, Clone)]
struct ChunkQueueReceiver<'a, 'b> {
    receiver: Receiver<(ChunkLocation, ChunkInfo<'a>)>,
    memory_limit: Option<(u64, &'b AtomicU64)>,
}

impl<'a> ChunkQueueReceiver<'a, '_> {
    fn recv(&self) -> Result<(ChunkLocation, ChunkInfo<'a>), crossbeam_channel::RecvError> {
        self.receiver.recv().map(|(loc, chunk_info)| {
            if let Some((_, counter)) = self.memory_limit {
                let chunk_size = loc.size().unwrap_or_else(|_| {
                    // fails only in case of filesystem-backed chunk. Just try to read from chunk
                    // info.
                    chunk_info.chunk_file_info().0
                });
                counter.fetch_sub(chunk_size, std::sync::atomic::Ordering::Release);
            }
            (loc, chunk_info)
        })
    }
}

#[derive(Debug)]
pub struct SophonInstaller {
    pub manifest: SophonManifestProto,
    pub download_info: SophonDownloadInfo,
    pub client: reqwest::blocking::Client,
    pub temp_folder: PathBuf,
    pub chunks_queue_data_limit: Option<u64>,
    pub last_file_suffix: Option<String>,
    pub check_free_space: bool,
    pub inplace: bool,
    pub chunks_in_mem: bool,
    pub skip_download_repair: bool,
    /// Will report broken files if this is enabled
    pub mode_repair: bool,
}

impl SophonInstaller {
    pub fn new(
        client: Client,
        download_info: &SophonDownloadInfo,
        temp_dir: impl AsRef<Path>,
    ) -> Result<Self, SophonError> {
        let manifest = get_download_manifest(&client, download_info)?;

        Ok(Self {
            client,
            manifest,
            download_info: download_info.clone(),
            temp_folder: temp_dir.as_ref().to_owned(),
            check_free_space: true,
            inplace: true,
            chunks_in_mem: true,
            chunks_queue_data_limit: Some(512 * 1024 * 1024),
            skip_download_repair: false,
            last_file_suffix: Some("globalgamemanagers".to_owned()),
            mode_repair: false,
        })
    }

    pub fn install(
        &self,
        output_folder: &Path,
        thread_count: usize,
        updater: impl Fn(Update) + Clone + Send,
    ) -> Result<(), SophonError> {
        if self.check_free_space {
            tracing::info!("Checking free space availability");

            let installed_size: u64 = self.download_info.stats.uncompressed_size.parse().unwrap();

            (updater)(Update::CheckingFreeSpace(self.temp_folder.clone()));

            #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
            if !self.chunks_in_mem {
                if let Some(queue_limit) = self.chunks_queue_data_limit {
                    Self::free_space_check(&updater, &self.temp_folder, queue_limit)?;
                }
            }

            (updater)(Update::CheckingFreeSpace(output_folder.to_owned()));

            let already_installed_size = fs_extra::dir::get_size(output_folder)?;

            let size_to_check = installed_size - already_installed_size;

            Self::free_space_check(&updater, output_folder, size_to_check)?;
        }

        tracing::info!("Downloading files");

        self.create_temp_dirs()?;

        let (download_threads, assembly_threads) = divide_threads(thread_count)?;

        self.install_multithreaded(
            download_threads,
            assembly_threads,
            output_folder,
            updater.clone(),
        );

        Ok(())
    }

    fn check_file_region(
        &self,
        game_folder: &Path,
        chunk_info: &ChunkInfo<'_>,
        file_info: &FileInfo<'_>,
    ) -> std::io::Result<bool> {
        tracing::debug!("entered region checking for some reason");
        let file_path = file_info.target_file_path(game_folder);

        let Ok(fs_metadata) = std::fs::metadata(&file_path) else {
            return Ok(false);
        };

        let file_size = fs_metadata.len();

        if file_size != file_info.file_manifest.asset_size {
            return Ok(false);
        }

        let mut file = File::open(&file_path)?;

        let region_md5 = file_region_hash_md5(
            &mut file,
            chunk_info.chunk_manifest.chunk_on_file_offset,
            chunk_info.chunk_manifest.chunk_size_decompressed,
        )?;

        Ok(region_md5 == chunk_info.chunk_manifest.chunk_decompressed_hash_md5)
    }

    fn install_multithreaded(
        &self,
        download_threads: NonZeroUsize,
        assembly_threads: NonZeroUsize,
        output_folder: impl AsRef<Path>,
        updater: impl Fn(Update) + Clone + Send,
    ) {
        tracing::debug!("Starting mutlithreaded download and install");

        let download_index = DownloadIndex::new(&self.download_info, &self.manifest);
        tracing::info!(
            "{} Chunks to download, {} Files to install, {} total bytes",
            download_index.chunks_used_in.len(),
            download_index.files.len(),
            prettify_bytes(download_index.total_bytes)
        );

        let game_folder = output_folder.as_ref();

        let queue_size = AtomicU64::default();
        let (chunk_sender, chunk_receiver) = crossbeam_channel::unbounded();
        let chunk_sender = ChunkQueueSender {
            sender: chunk_sender,
            memory_limit: self.chunks_queue_data_limit.map(|lim| (lim, &queue_size)),
        };
        let chunk_receiver = ChunkQueueReceiver {
            receiver: chunk_receiver,
            memory_limit: self.chunks_queue_data_limit.map(|lim| (lim, &queue_size)),
        };

        let mut redownload_set = HashSet::with_capacity(download_index.chunks_used_in.len());
        let mut chunk_dedupe_set = HashSet::with_capacity(download_index.chunks_used_in.len());

        let total_files = download_index.total_files();
        (updater)(Update::CheckingFiles { total_files });

        let mut passed_files = 0_u64;

        let download_queue = tracing::info_span!("Building chunk queue").in_scope(|| {
            VecDeque::from_iter(
                download_index
                    .files
                    .values()
                    .filter(|file_info| {
                        let check_res = check_file(
                            file_info.target_file_path(game_folder),
                            file_info.file_manifest.asset_size,
                            &file_info.file_manifest.asset_hash_md5,
                        )
                        .unwrap_or(false);
                        if check_res {
                            passed_files += 1;
                            (updater)(Update::CheckingFilesProgress {
                                passed: passed_files,
                                total: total_files,
                            });
                        }
                        !check_res
                    })
                    .flat_map(|file_info| {
                        file_info
                            .chunks_iter()
                            .map(move |chinfo| (file_info, chinfo))
                    })
                    .filter_map(move |(file_info, chunk_info)| {
                        if redownload_set.contains(&chunk_info.chunk_manifest.chunk_name) {
                            // This chunk was already checked, and included in the queue. No need
                            // for duplicates, as they will be filtered
                            // in the next filter call
                            return None;
                        }
                        if !self
                            .check_file_region(game_folder, &chunk_info, file_info)
                            .unwrap_or(false)
                        {
                            if self.mode_repair {
                                tracing::error!(
                                    "Broken file detected: {}",
                                    file_info.file_manifest.asset_name
                                )
                            }
                            redownload_set.insert(&chunk_info.chunk_manifest.chunk_name);
                            Some(chunk_info)
                        } else {
                            None
                        }
                    })
                    .filter(move |chunk_info| {
                        chunk_dedupe_set.insert(&chunk_info.chunk_manifest.chunk_name)
                    }),
            )
        });

        // Early exit if all files are already downloaded
        if self.skip_download_repair || download_queue.is_empty() {
            return;
        }

        let actual_download = download_queue
            .iter()
            .map(|chunk_info| chunk_info.chunk_manifest.chunk_size)
            .sum::<u64>();
        let download_queue = Mutex::new(download_queue);

        (updater)(Update::DownloadingStarted {
            location: game_folder.to_owned(),
            total_bytes: download_index.total_bytes,
            total_files,
        });

        (updater)(download_index.add_msg_files(0));

        // add all the skipped chunks to the download progress so that the displayed total is more
        // accurate
        (updater)(download_index.add_msg_bytes(download_index.total_bytes - actual_download));

        tracing::debug!("Spawning threads");
        std::thread::scope(|scope| {
            // References to some variables because the spawned threads capture variables
            let index_ref = &download_index;
            let download_queue_ref = &download_queue;

            // Downloading threads
            for i in 0..download_threads.get() {
                let sender_clone = chunk_sender.clone();
                let updater_clone = updater.clone();

                scope.spawn(move || {
                    let _span = tracing::debug_span!("Download thread", thread_num = i).entered();
                    self.artifact_download_loop(
                        download_queue_ref,
                        Some(sender_clone),
                        index_ref,
                        updater_clone,
                    );
                });
            }

            // Assembly threads
            for i in 0..assembly_threads.get() {
                let updater_clone = updater.clone();
                let index_ref = &download_index;
                let receiver_clone = chunk_receiver.clone();

                scope.spawn(move || {
                    let _span = tracing::debug_span!("Patching thread", thread_num = i).entered();
                    self.file_assembly_loop(game_folder, updater_clone, index_ref, receiver_clone);
                });
            }

            // drop the original sender and receiver so teh rest is in the spawned threads
            drop(chunk_sender);
            drop(chunk_receiver);
        });

        if let Some(last_file_suffix) = &self.last_file_suffix {
            self.last_file_handle(game_folder, &updater, &download_index, last_file_suffix);
        }

        (updater)(Update::DownloadingFinished);
    }

    pub fn pre_download(
        &self,
        thread_count: usize,
        updater: impl Fn(Update) + Clone + Send + 'static,
    ) -> Result<(), SophonError> {
        if self.check_free_space {
            tracing::info!("Checking free space availability");
            (updater)(Update::CheckingFreeSpace(self.temp_folder.clone()));

            let download_size = self.download_info.stats.compressed_size.parse().unwrap();

            Self::free_space_check(updater.clone(), &self.temp_folder, download_size)?;
        }

        self.create_temp_dirs()?;

        self.predownload_multithreaded(thread_count, updater);

        let marker_file_path = self.downloading_temp().join(".predownloadcomplete");
        File::create(marker_file_path)?;

        Ok(())
    }

    // TODO: adjust chunk location and downlaod logic to respect chunks being on disk for
    // predownload
    fn predownload_multithreaded(
        &self,
        thread_count: usize,
        updater: impl Fn(Update) + Clone + Send + 'static,
    ) {
        tracing::debug!("Starting multithreaded predownload");

        let download_index = DownloadIndex::new(&self.download_info, &self.manifest);
        tracing::info!(
            "{} Chunks to download, {} total bytes",
            download_index.chunks_used_in.len(),
            prettify_bytes(download_index.total_bytes)
        );

        (updater)(download_index.add_msg_files(0));
        (updater)(download_index.add_msg_bytes(0));

        let mut chunk_dedupe_set = HashSet::with_capacity(download_index.chunks_used_in.len());
        let download_queue = Mutex::new(VecDeque::from_iter(
            download_index
                .files
                .values()
                .flat_map(|file_info| file_info.chunks_iter())
                .filter(move |chunk_info| {
                    chunk_dedupe_set.insert(&chunk_info.chunk_manifest.chunk_name)
                }),
        ));

        tracing::debug!("Starting download");
        std::thread::scope(|scope| {
            for _ in 0..thread_count {
                let updater_clone = updater.clone();
                scope.spawn(|| {
                    self.artifact_download_loop(
                        &download_queue,
                        None,
                        &download_index,
                        updater_clone,
                    );
                });
            }
        });

        (updater)(Update::DownloadingFinished);
    }

    /// Loops over the tasks and retries and tries to download them, pushing
    /// onto the file assembly queue if all the chunks for a file succeeded.
    /// If both the tasks iterator and the retries queue don't return
    /// anything, checks if they are empty and then checks if there are any
    /// unfinished chunks and waits for either all chunks to finish applying
    /// or a new retry being pushed onto the queue.
    fn artifact_download_loop<'a, 'b>(
        &self,
        task_queue: &Mutex<VecDeque<ChunkInfo<'a>>>,
        assembly_queue: Option<ChunkQueueSender<'a, 'b>>,
        download_index: &'a DownloadIndex<'a>,
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
            // Check if the file already exists on disk and if it does, skip re-downloading
            let artifact_path = self.tmp_artifact_file_path(&task);

            let res = if artifact_path.exists() {
                tracing::debug!(artifact = ?artifact_path, "Artifact already exists, skipping download");
                Ok(ChunkLocation::Filesystem(artifact_path))
            } else {
                self.download_artifact(&task)
            };

            let (chunk_size, chunk_hash) = task.chunk_file_info();

            let res = res.and_then(|loc| {
                if !{ loc.check(chunk_size, chunk_hash)? } {
                    Err(SophonError::ChunkHashMismatch {
                        expected: chunk_hash.to_owned(),
                        got: loc.hash()?,
                    })
                } else {
                    Ok(loc)
                }
            });

            match res {
                Ok(loc) => {
                    (updater)(download_index.add_msg_bytes(chunk_size));
                    if let Some(file_queue) = &assembly_queue {
                        // Send downlaoded chunk to the assembly threads, the body is error
                        // handling.
                        if let Err(err) =
                            file_queue.send_timeout((loc, task), Duration::from_secs(10))
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
                        };
                    }
                }
                Err(err) => {
                    tracing::error!(
                        chunk_name = task.chunk_manifest.chunk_name,
                        ?err,
                        "Failed to download chunk",
                    );
                    let _ = std::fs::remove_file(self.tmp_artifact_file_path(&task));
                    (updater)(Update::DownloadingError(err));
                    download_index.process_download_fail(task, task_queue, &updater);
                }
            }
        }
    }

    // instrumenting to maybe try and see how much time it takes to download and
    // save
    #[tracing::instrument(level = "debug", err, skip_all, fields(chunk = task.chunk_manifest.chunk_name, download_size = task.chunk_file_info().0))]
    fn download_artifact(&self, task: &ChunkInfo) -> Result<ChunkLocation, SophonError> {
        let download_url = task.download_url();
        let out_file_path = self.tmp_artifact_file_path(task);

        let (chunk_size, _) = task.chunk_file_info();

        let resp = self.client.get(download_url).send()?.error_for_status()?;

        // In theory, can catch the size mismatch before writing to the disk?
        #[allow(clippy::collapsible_if, reason = "only collapsible in Rust >= 1.88.0")]
        if let Some(length) = resp.content_length() {
            if length != chunk_size {
                return Err(SophonError::DownloadSizeMismatch {
                    name: "Content Length",
                    expected: chunk_size,
                    got: length,
                });
            }
        }

        let bytes = resp.bytes()?;

        let recvd = bytes.len() as u64;
        if recvd != chunk_size {
            return Err(SophonError::DownloadSizeMismatch {
                name: "Request Data",
                expected: chunk_size,
                got: recvd,
            });
        }

        if self.chunks_queue_data_limit.is_some() {
            Ok(ChunkLocation::Memory(bytes))
        } else {
            std::fs::write(&out_file_path, bytes)?;

            Ok(ChunkLocation::Filesystem(out_file_path))
        }
    }

    fn last_file_handle<'a>(
        &self,
        game_folder: &Path,
        updater: impl Fn(Update),
        download_index: &DownloadIndex<'a>,
        last_file_suffix: &str,
    ) {
        let last_file_path = self.downloading_temp().join("last_file.tmp");
        if last_file_path.exists() {
            // todo: global OnceLock/Mutex<Vec<FileInfo>> for last file(s) rather than this mess
            let last_file_task = download_index
                .files
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

    fn file_assembly_loop<'a>(
        &self,
        game_folder: &Path,
        updater: impl Fn(Update),
        download_index: &DownloadIndex<'a>,
        queue: ChunkQueueReceiver<'a, '_>,
    ) {
        while let Ok((loc, downloaded_chunk)) = queue.recv() {
            let files_with_chunk = download_index
                .chunks_used_in
                .get(&downloaded_chunk.chunk_manifest.chunk_name)
                .expect("All chunks must be indexed");
            for file_id in files_with_chunk {
                self.file_assembly_partial_handler(
                    &downloaded_chunk,
                    &loc,
                    file_id,
                    download_index,
                    game_folder,
                    &updater,
                );
            }
            //self.file_assembly_handler(downloaded_chunk, download_index, game_folder, &updater);

            loc.cleanup();
        }
    }

    fn file_assembly_partial_handler<'a>(
        &self,
        downloaded_chunk: &ChunkInfo<'a>,
        chunk_location: &ChunkLocation,
        file_id: &String,
        downloading_index: &DownloadIndex<'a>,
        game_folder: &Path,
        updater: impl Fn(Update),
    ) {
        let file_info = downloading_index
            .files
            .get(file_id)
            .expect("All files must be indexed");
        let target_file_path = if let Some(last_file_suffix) = &self.last_file_suffix {
            if file_info
                .file_manifest
                .asset_name
                .ends_with(last_file_suffix)
            {
                self.downloading_temp().join("last_file.tmp")
            } else {
                file_info.target_file_path(game_folder)
            }
        } else {
            file_info.target_file_path(game_folder)
        };
        let working_file = if !self.inplace {
            self.tmp_downloading_file_path(file_info)
        } else {
            let _ = ensure_parent(&target_file_path);
            target_file_path.clone()
        };
        // in case a chunk is repeated multiple times in a file
        // iterator instead of a singular find
        let mut chunk_infos_for_file = file_info
            .file_manifest
            .asset_chunks
            .iter()
            .filter(|chinfo| chinfo.chunk_name == downloaded_chunk.chunk_manifest.chunk_name);

        let res = if let Ok(true) = check_file(
            &target_file_path,
            file_info.file_manifest.asset_size,
            &file_info.file_manifest.asset_hash_md5,
        ) {
            tracing::debug!(file = ?target_file_path, "File appears to be already downloaded");
            Ok(true)
        } else {
            chunk_infos_for_file
                .try_for_each(|chunk_info| {
                    self.file_assembly_partial(
                        &working_file,
                        chunk_location,
                        &ChunkInfo {
                            chunk_manifest: chunk_info,
                            download_info: downloaded_chunk.download_info,
                            retries_left: downloaded_chunk.retries_left,
                        },
                        file_info,
                    )
                })
                // Check if the temporary file is completed
                .and_then(|_| {
                    check_file(
                        &working_file,
                        file_info.file_manifest.asset_size,
                        &file_info.file_manifest.asset_hash_md5,
                    )
                    .map_err(SophonError::IoError)
                })
                // If it is, finalize and remove temporary file
                // I know it basically checks twice
                .and_then(|file_valid| {
                    if !self.inplace && file_valid {
                        finalize_file(
                            &working_file,
                            &target_file_path,
                            file_info.file_manifest.asset_size,
                            &file_info.file_manifest.asset_hash_md5,
                        )
                        .inspect_err(|err| {
                            tracing::error!(?err, "Error during file finalization")
                        })?;

                        let _ = std::fs::remove_file(&working_file);
                    }
                    Ok(file_valid)
                })
        };

        match res {
            Ok(true) => {
                tracing::debug!(
                    "Successfully downloaded `{}`",
                    file_info.file_manifest.asset_name
                );
                (updater)(downloading_index.add_msg_files(1))
            }
            Ok(false) => {
                tracing::debug!(
                    chunk = downloaded_chunk.chunk_manifest.chunk_name,
                    file = file_info.file_manifest.asset_name,
                    "Applied chunk to the file"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    file = file_info.file_manifest.asset_name,
                    "Error during file assembly"
                );
                (updater)(Update::DownloadingError(e));
                self.cleanup_on_fail(file_info);
            }
        }
    }

    fn cleanup_on_fail(&self, task: &FileInfo) {
        let _ = std::fs::remove_file(self.tmp_downloading_file_path(task));
    }

    #[tracing::instrument(level = "debug", err, skip_all, fields(
            asset_name = task.file_manifest.asset_name,
            asset_hash = task.file_manifest.asset_hash_md5,
            asset_size = task.file_manifest.asset_size,
            chunk_name = chunk_info.chunk_manifest.chunk_name
        )
    )]
    fn file_assembly_partial(
        &self,
        working_file: &Path,
        chunk_location: &ChunkLocation,
        chunk_info: &ChunkInfo<'_>,
        task: &FileInfo,
    ) -> Result<(), SophonError> {
        #[allow(
            clippy::suspicious_open_options,
            reason = "File length is set right after opening, which truncates or extends it"
        )]
        let mut output_file = OpenOptions::new()
            .create(true)
            //.truncate(true)
            .write(true)
            .open(working_file)?;
        output_file.set_len(task.file_manifest.asset_size)?;

        // not checking written data. might be useful in debugging. not doing it though.
        match chunk_location {
            ChunkLocation::Filesystem(chunk_path) => {
                self.write_chunk_fs_to_file(chunk_path, chunk_info, &mut output_file)?;
            }
            ChunkLocation::Memory(chunk_buf) => {
                self.write_chunk_mem_to_file(chunk_buf, chunk_info, &mut output_file)?;
            }
        }

        Ok(())
    }

    fn write_chunk_fs_to_file<W: Write + Seek>(
        &self,
        chunk_path: &Path,
        chunk_info: &ChunkInfo,
        dest_file: &mut W,
    ) -> std::io::Result<u64> {
        dest_file.seek(std::io::SeekFrom::Start(
            chunk_info.chunk_manifest.chunk_on_file_offset,
        ))?;
        if chunk_info.is_compressed() {
            Self::write_artifact_to_file_zstd(dest_file, chunk_path)
        } else {
            Self::write_artifact_to_file(dest_file, chunk_path)
        }
    }

    fn write_chunk_mem_to_file<W: Write + Seek>(
        &self,
        chunk_buf: &[u8],
        chunk_info: &ChunkInfo,
        dest_file: &mut W,
    ) -> std::io::Result<u64> {
        dest_file.seek(std::io::SeekFrom::Start(
            chunk_info.chunk_manifest.chunk_on_file_offset,
        ))?;
        if chunk_info.is_compressed() {
            Self::write_artifact_to_file_zstd_mem(dest_file, chunk_buf)
        } else {
            Self::write_artifact_to_file_mem(dest_file, chunk_buf)
        }
    }

    fn write_artifact_to_file<W: Write>(
        dest_file: &mut W,
        artifact_path: &Path,
    ) -> std::io::Result<u64> {
        let mut artifact_file = File::open(artifact_path)?;
        std::io::copy(&mut artifact_file, dest_file)
    }

    fn write_artifact_to_file_zstd<W: Write>(
        dest_file: &mut W,
        artifact_path: &Path,
    ) -> std::io::Result<u64> {
        let artifact_file = File::open(artifact_path)?;
        let mut zstd_decoder = zstd::Decoder::new(artifact_file)?;
        std::io::copy(&mut zstd_decoder, dest_file)
    }

    fn write_artifact_to_file_mem<W: Write>(
        dest_file: &mut W,
        chunk_buf: &[u8],
    ) -> std::io::Result<u64> {
        dest_file
            .write_all(chunk_buf)
            .map(|_| chunk_buf.len() as u64)
    }

    fn write_artifact_to_file_zstd_mem<W: Write>(
        dest_file: &mut W,
        chunk_buf: &[u8],
    ) -> std::io::Result<u64> {
        let mut zstd_decoder = zstd::Decoder::new(chunk_buf)?;
        std::io::copy(&mut zstd_decoder, dest_file)
    }

    fn free_space_check(
        updater: impl Fn(Update),
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

    #[inline]
    pub fn with_free_space_check(mut self, check: bool) -> Self {
        self.check_free_space = check;

        self
    }

    #[inline]
    pub fn with_temp_folder(mut self, temp_folder: PathBuf) -> Self {
        self.temp_folder = temp_folder;

        self
    }

    /// Folder to temporarily store files being downloaded
    #[inline]
    pub fn downloading_temp(&self) -> PathBuf {
        self.temp_folder
            .join(format!("downloading-{}", self.download_info.matching_field))
    }

    fn tmp_downloading_file_path(&self, file_info: &FileInfo) -> PathBuf {
        self.tmp_downloading_folder().join(file_info.tmp_filename())
    }

    /// Folder to temporarily store chunks
    #[inline]
    fn chunk_temp_folder(&self) -> PathBuf {
        self.downloading_temp().join("chunks")
    }

    #[inline]
    fn tmp_downloading_folder(&self) -> PathBuf {
        self.downloading_temp().join("files-in-progress")
    }

    fn tmp_artifact_file_path(&self, chunk_info: &ChunkInfo) -> PathBuf {
        self.chunk_temp_folder().join(chunk_info.ondisk_filename())
    }

    /// Create all needed sub-directories in the temp folder
    fn create_temp_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.downloading_temp())?;
        std::fs::create_dir_all(self.chunk_temp_folder())?;
        std::fs::create_dir_all(self.tmp_downloading_folder())?;

        Ok(())
    }
}
