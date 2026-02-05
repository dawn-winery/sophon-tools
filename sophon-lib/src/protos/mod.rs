include!(concat!(env!("OUT_DIR"), "/protos.rs"));

impl SophonManifestProto {
    pub fn total_bytes_compressed(&self) -> u64 {
        self.assets
            .iter()
            .flat_map(|asset| &asset.asset_chunks)
            .map(|asset_chunk| asset_chunk.chunk_size)
            .sum()
    }

    pub fn total_bytes_decompressed(&self) -> u64 {
        self.assets
            .iter()
            .flat_map(|asset| &asset.asset_chunks)
            .map(|asset_chunk| asset_chunk.chunk_size_decompressed)
            .sum()
    }

    pub fn total_chunks(&self) -> u64 {
        self.assets
            .iter()
            .flat_map(|asset| &asset.asset_chunks)
            .count() as u64
    }

    pub fn total_files(&self) -> u64 {
        self.assets.len() as u64
    }
}
