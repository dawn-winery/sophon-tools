// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@vk.com>
//                     "John the Cooling Fan"
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::io::{Cursor, Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::io::{
    BufReader, BufWriter, AsyncReadExt, AsyncWriteExt, AsyncSeekExt
};
use tokio::fs::File;
use tokio::process::Command;

const HPATCHZ: &[u8] = include_bytes!("../external/hpatchz/hpatchz");

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HdiffPatcher(PathBuf);

impl Default for HdiffPatcher {
    #[inline]
    fn default() -> Self {
        Self(PathBuf::from("hpatchz"))
    }
}

impl From<PathBuf> for HdiffPatcher {
    #[inline]
    fn from(path: PathBuf) -> Self {
        Self(path)
    }
}

impl HdiffPatcher {
    /// Write bundled hpatchz binary to the temp directory and point the patcher
    /// to use it.
    pub async fn export() -> std::io::Result<Self> {
        let hash = seahash::hash(HPATCHZ);

        let path = std::env::temp_dir()
            .join(format!("hpatchz-{hash:0x}"));

        if !path.is_file() {
            #[cfg(feature = "tracing")]
            tracing::debug!(?path, "export bundled hpatchz binary");

            tokio::fs::write(&path, HPATCHZ).await?;
        }

        Ok(Self(path))
    }

    /// Apply patch to the input file and save it under the output path. If
    /// `Ok(false)` is returned, then the patch was not applied.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = tracing::Level::DEBUG, skip(self), ret)
    )]
    pub async fn patch(
        &self,
        input: &Path,
        patch: &Path,
        output: &Path
    ) -> std::io::Result<bool> {
        let output = Command::new(&self.0)
            .arg("-f")
            .arg(input)
            .arg(patch)
            .arg(output)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await?;

        Ok(String::from_utf8_lossy(&output.stdout).contains("patch ok!"))
    }

    /// Extract new file stored in hdiff13 patch. Return `Ok(false)` if given
    /// patch is not a valid hdiff13 new file.
    pub async fn extract(
        patch: &Path,
        output: &Path
    ) -> std::io::Result<bool> {
        // Open patch file.
        let mut patch = BufReader::new(File::open(patch).await?);

        // Check if the given patch is really an hdiff13.
        let mut header = [0; 128];

        patch.read_exact(&mut header).await?;

        if &header[..8] != b"HDIFF13\0" {
            return Ok(false);
        }

        let mut header = if &header[8..13] == b"ZSTD\0" {
            &header[13..]
        } else if &header[8..9] != b"\0\0" {
            &header[9..]
        } else {
            return Ok(false);
        };

        fn read_varint(buf: &[u8]) -> Option<(u64, &[u8])> {
            let mut varint = 0_u64;
            let mut i = 0;

            loop {
                varint |= (buf[i] & 0b01111111) as u64;

                if buf[i] & 0b10000000 == 0 {
                    return Some((varint, &buf[i + 1..]));
                }

                varint <<= 7;
                i += 1;
            }
        }

        for _ in 0..10 {
            let Some((_, new_header)) = read_varint(header) else {
                return Ok(false);
            };

            header = new_header;
        }

        let Some((decompressed_buf_size, header)) = read_varint(header) else {
            return Ok(false);
        };

        let Some((compressed_buf_size, _)) = read_varint(header) else {
            return Ok(false);
        };

        // Open output file.
        let mut output = BufWriter::new(
            File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(output)
                .await?
        );

        let (offset, is_compressed) = if compressed_buf_size == 0 {
            (decompressed_buf_size, false)
        } else {
            (compressed_buf_size, true)
        };

        patch.seek(SeekFrom::End(-(offset as i64))).await?;

        if !is_compressed {
            tokio::io::copy(&mut patch, &mut output).await?;
        }

        else {
            // FIXME: inefficient af
            let mut patch_buf = Vec::with_capacity(compressed_buf_size as usize);

            patch.read_to_end(&mut patch_buf).await?;

            drop(patch);

            let mut decoder = ruzstd::decoding::StreamingDecoder::new(
                Cursor::new(patch_buf)
            ).map_err(std::io::Error::other)?;

            let mut output_buf = Vec::with_capacity(
                decompressed_buf_size as usize
            );

            decoder.read_to_end(&mut output_buf)?;

            output.write_all(&output_buf).await?;
        }

        output.flush().await?;

        Ok(true)
    }
}
