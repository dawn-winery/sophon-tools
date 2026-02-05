use indicatif::HumanBytes;
use sophon_lib::{
    api::schemas::{
        game_branches::{GameBranchInfo, GameBranches, PackageInfo},
        game_configs::{GameConfigs, GameLaunchConfig},
        game_scan_info::{GameExeHash, GameScanInfo, ScanInfo},
        sophon_diff::{SophonDiff, SophonDiffs},
        sophon_manifests::{
            DownloadInfo, Manifest, ManifestStats, SophonDownloadInfo, SophonDownloads,
        },
    },
    protos::{
        SophonManifestAssetChunk, SophonManifestAssetProperty, SophonManifestProto,
        SophonPatchAssetChunk, SophonPatchAssetProperty, SophonPatchProto, SophonUnusedAssetFile,
    },
};

fn prettify_bytes_str(bytes_str: &str) -> String {
    HumanBytes(bytes_str.parse().expect("Valid number returned by the API")).to_string()
}

pub trait PrettyPrint {
    fn pretty_print(&self);
}

impl PrettyPrint for GameBranches {
    fn pretty_print(&self) {
        println!("Game Branches:");
        for game_branch in &self.game_branches {
            print!("\n\n");
            game_branch.pretty_print()
        }
    }
}

impl PrettyPrint for GameBranchInfo {
    fn pretty_print(&self) {
        println!("Game codename: {}", self.game.biz);
        println!("Branch id: `{}`", self.game.id);
        if let Some(main_package) = &self.main {
            println!();
            main_package.pretty_print();
        }
        if let Some(preload_package) = &self.pre_download {
            println!();
            println!("*** PREDOWNLOAD AVAILABLE ***");
            preload_package.pretty_print();
        }

        if self.main.is_none() && self.main.is_none() {
            println!("No packages available")
        }
    }
}

impl PrettyPrint for PackageInfo {
    fn pretty_print(&self) {
        println!("Package `{}` (Branch {})", self.package_id, self.branch);
        println!("  Access password: `{}`", self.password);
        println!("  Version tag: {}", self.tag);
        if !self.diff_tags.is_empty() {
            println!();
            println!(
                "Updates available from versions: {}",
                self.diff_tags.join(", ")
            );
        }
        if !self.categories.is_empty() {
            println!();
            println!("Categories:");
            for category in &self.categories {
                println!(
                    "  - `{}`, (id {})",
                    category.matching_field, category.category_id
                )
            }
        }
    }
}

impl PrettyPrint for SophonDownloads {
    fn pretty_print(&self) {
        println!("Build id: {}", self.build_id);
        println!("  Version tag: {}", self.tag);
        println!();

        if self.manifests.is_empty() {
            println!("No manifests associated");
        } else {
            for manifest in &self.manifests {
                println!();
                manifest.pretty_print()
            }
        }
    }
}

impl PrettyPrint for SophonDownloadInfo {
    fn pretty_print(&self) {
        println!("Name: {}", self.category_name);
        println!("  Matching id: {}", self.matching_field);
        println!("  ID: {}", self.category_id);
        println!("Manifest (protobuf) info:");
        self.manifest.pretty_print();
        println!("Manifest download info:");
        self.manifest_download.pretty_print();
        println!("Chunk download info:");
        self.chunk_download.pretty_print();
        println!("Download stats:");
        self.stats.pretty_print();
        if self.stats != self.deduplicated_stats {
            println!("!!! Stats do not match deduplicated(?) stats, somehow !!!");
            println!("Deduplicated (repeated?) download stats:");
            self.deduplicated_stats.pretty_print();
        }
    }
}

impl PrettyPrint for Manifest {
    fn pretty_print(&self) {
        println!("  ID: {}", self.id);
        println!("  Checksum: {}", self.checksum);
        println!(
            "  Compressed size: {}",
            prettify_bytes_str(&self.compressed_size)
        );
        println!(
            "  Uncompressed size: {}",
            prettify_bytes_str(&self.uncompressed_size)
        );
    }
}

impl PrettyPrint for DownloadInfo {
    fn pretty_print(&self) {
        if self.url_suffix.is_empty() {
            println!("  Full base URL: {}", self.url_prefix);
        } else {
            println!("  URL prefix: {}", self.url_prefix);
            println!("  URL suffix: {}", self.url_suffix);
            println!("  Full base url: {}{}", self.url_prefix, self.url_suffix);
        }
        println!("  Encryption flags number: {}", self.encryption);
        if self.password.is_empty() {
            println!("  No password for access")
        } else {
            println!("  Access password: {}", self.password)
        }
        println!("  Compression flags number: {}", self.compression);
    }
}

impl PrettyPrint for ManifestStats {
    fn pretty_print(&self) {
        println!("  File count: {}", self.file_count);
        println!("  Chunk count: {}", self.chunk_count);
        println!(
            "  Compressed_size: {}",
            prettify_bytes_str(&self.compressed_size)
        );
        println!(
            "  Uncompressed size: {}",
            prettify_bytes_str(&self.uncompressed_size)
        );
    }
}

impl PrettyPrint for GameScanInfo {
    fn pretty_print(&self) {
        println!("All games scan info:");
        for game_info in &self.game_scan_info {
            println!();
            game_info.pretty_print()
        }
    }
}

impl PrettyPrint for ScanInfo {
    fn pretty_print(&self) {
        println!("Game id: `{}`", self.game_id);
        for game_exe in &self.game_exe_list {
            game_exe.pretty_print();
        }
    }
}

impl PrettyPrint for GameExeHash {
    fn pretty_print(&self) {
        println!("{}: `{}`", self.version, self.md5)
    }
}

impl PrettyPrint for SophonManifestProto {
    fn pretty_print(&self) {
        println!("assets:");
        for asset in &self.assets {
            println!();
            asset.pretty_print()
        }
    }
}

impl PrettyPrint for SophonManifestAssetProperty {
    fn pretty_print(&self) {
        println!("Filename: \"{}\"", self.asset_name);
        print!("  Type number: {}", self.asset_type);
        if self.asset_type == 0 {
            println!(" (file)");
        } else {
            println!();
        }
        println!("  Size: {}", HumanBytes(self.asset_size));
        println!("  MD5 hash: `{}`", self.asset_hash_md5);
        if !self.asset_chunks.is_empty() {
            println!("  Chunks:");
            for chunk_info in &self.asset_chunks {
                println!();
                chunk_info.pretty_print()
            }
        } else {
            println!("  Asset has no chunks")
        }
    }
}

impl PrettyPrint for SophonManifestAssetChunk {
    fn pretty_print(&self) {
        println!("    Name: `{}`", self.chunk_name);
        println!(
            "    Compressed: size {}, MD5 hash `{}`, unknown field `{:#018x}`",
            HumanBytes(self.chunk_size),
            self.chunk_compressed_hash_md5,
            self.chunk_compressed_hash_xxh
        );
        println!(
            "    Decompressed: size {}, MD5 hash `{}`",
            HumanBytes(self.chunk_size_decompressed),
            self.chunk_decompressed_hash_md5
        );
        println!("    On-file offset: {:#x}", self.chunk_on_file_offset);
    }
}

impl PrettyPrint for SophonDiffs {
    fn pretty_print(&self) {
        println!("Build id: {}", self.build_id);
        println!("  Patch id: {}", self.patch_id);
        println!("  Version tag: {}", self.tag);
        println!();

        if self.manifests.is_empty() {
            println!("No manifests associated");
        } else {
            for manifest in &self.manifests {
                println!();
                manifest.pretty_print()
            }
        }
    }
}

impl PrettyPrint for SophonDiff {
    fn pretty_print(&self) {
        println!("Name: {}", self.category_name);
        println!("  Matching id: {}", self.matching_field);
        println!("  ID: {}", self.category_id);
        println!("Manifest (protobuf) info:");
        self.manifest.pretty_print();
        println!("Manifest download info:");
        self.manifest_download.pretty_print();
        println!("Patch download info:");
        self.diff_download.pretty_print();
        for (ver, stats) in &self.stats {
            println!("Download stats (from {ver}):");
            stats.pretty_print();
        }
    }
}

impl PrettyPrint for SophonPatchProto {
    fn pretty_print(&self) {
        println!("Patched assets:");
        for asset in &self.patch_assets {
            asset.pretty_print();
        }
        println!();
        println!();
        if self.unused_assets.is_empty() {
            println!("No unused assets that need deletion")
        } else {
            println!("Unused assets:");
            for (ver, unused_files) in &self.unused_assets {
                if unused_files.assets.is_empty() {
                    println!("  None from version {ver}")
                } else {
                    // todo
                    let verbose = false;
                    println!("  From version {ver}:");
                    if verbose {
                        for unused_file in &unused_files.assets {
                            unused_file.pretty_print();
                        }
                    } else {
                        println!("    ({} unused files not shown)", unused_files.assets.len())
                    }
                }
            }
        }
    }
}

impl PrettyPrint for SophonPatchAssetProperty {
    fn pretty_print(&self) {
        if !self.asset_patch_chunks.is_empty() {
            println!();
            println!("  File {}", self.asset_name);
            println!(
                "  Size: {}; Hash: {}",
                HumanBytes(self.asset_size),
                self.asset_hash_md5
            );
            if self.asset_patch_chunks.is_empty() {
                println!("  NO PATCHES (???)");
            } else {
                for (ver, chunk) in &self.asset_patch_chunks {
                    println!("  Patch from version {ver}:");
                    chunk.pretty_print()
                }
            }
            println!();
        } else {
            println!(
                "  File {}; Size {}; Hash {}; no chunks",
                self.asset_name,
                HumanBytes(self.asset_size),
                self.asset_hash_md5
            );
        }
    }
}

impl PrettyPrint for SophonPatchAssetChunk {
    fn pretty_print(&self) {
        println!(
            "    Chunk name: {}; size: {}; hash: {}",
            self.patch_name,
            HumanBytes(self.patch_size),
            self.patch_md5
        );
        println!("    Build id: {}", self.build_id);
        println!("    Version tag: {}", self.version_tag);
        println!("    Patch size: {}", HumanBytes(self.patch_length));
        println!(
            "    Patch location in chunk (hex): 0x{:x}-0x{:x}",
            self.patch_offset,
            self.patch_offset + self.patch_length - 1
        );
        if self.original_file_name.is_empty() {
            println!("    Original file does not exist, the file is new");
        } else {
            println!("    Original file name: {}", self.original_file_name);
            println!(
                "    Original file size: {}; hash: {}",
                HumanBytes(self.original_file_length),
                self.original_file_md5
            );
        }
    }
}

impl PrettyPrint for SophonUnusedAssetFile {
    fn pretty_print(&self) {
        let Self {
            file_name,
            file_size,
            file_md5,
            ..
        } = self;
        println!(
            "    File {file_name}; size {}; hash {file_md5}",
            HumanBytes(*file_size)
        );
    }
}

impl PrettyPrint for GameConfigs {
    fn pretty_print(&self) {
        println!("Game Launch Configs:");
        for launch_config in &self.launch_configs {
            println!();
            launch_config.pretty_print()
        }
    }
}

impl PrettyPrint for GameLaunchConfig {
    fn pretty_print(&self) {
        println!("Game biz: `{}`; id: `{}`", self.game.biz, self.game.id);
        println!("  Executable file name: {}", self.exe_file_name);
        println!("  Default download mode: {}", self.default_download_mode);
    }
}
