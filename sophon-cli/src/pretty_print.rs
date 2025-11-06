use indicatif::HumanBytes;
use sophon_lib::{
    api::schemas::{
        game_branches::{GameBranchInfo, GameBranches, PackageInfo},
        game_scan_info::{GameExeHash, GameScanInfo, ScanInfo},
        sophon_manifests::{
            DownloadInfo, Manifest, ManifestStats, SophonDownloadInfo, SophonDownloads,
        },
    },
    protos::SophonManifest::{
        SophonManifestAssetChunk, SophonManifestAssetProperty, SophonManifestProto,
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
        println!("Assets:");
        for asset in &self.Assets {
            println!();
            asset.pretty_print()
        }
    }
}

impl PrettyPrint for SophonManifestAssetProperty {
    fn pretty_print(&self) {
        println!("Filename: \"{}\"", self.AssetName);
        print!("  Type number: {}", self.AssetType);
        if self.AssetType == 0 {
            println!(" (file)");
        } else {
            println!();
        }
        println!("  Size: {}", HumanBytes(self.AssetSize));
        println!("  MD5 hash: `{}`", self.AssetHashMd5);
        if !self.AssetChunks.is_empty() {
            println!("  Chunks:");
            for chunk_info in &self.AssetChunks {
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
        println!("    Name: `{}`", self.ChunkName);
        println!(
            "    Compressed: size {}, MD5 hash `{}`, unknown field `{:#018x}`",
            HumanBytes(self.ChunkSize),
            self.ChunkCompressedHashMd5,
            self.ChunkCompressedHashXxh
        );
        println!(
            "    Decompressed: size {}, MD5 hash `{}`",
            HumanBytes(self.ChunkSizeDecompressed),
            self.ChunkDecompressedHashMd5
        );
        println!("    On-file offset: {:#x}", self.ChunkOnFileOffset);
    }
}
