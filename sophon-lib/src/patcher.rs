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

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

use md5::{Md5, Digest};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const HPATCHZ: &[u8] = include_bytes!("../external/hpatchz/hpatchz-linux64");

#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
const HPATCHZ: &[u8] = include_bytes!("../external/hpatchz/hpatchz-windows64.exe");

#[cfg(target_os = "macos")]
const HPATCHZ: &[u8] = include_bytes!("../external/hpatchz/hpatchz-macos");

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HdiffPatcher(PathBuf);

impl Default for HdiffPatcher {
    #[inline]
    fn default() -> Self {
        #[cfg(not(target_os = "windows"))]
        return Self(PathBuf::from("hpatchz"));

        #[cfg(target_os = "windows")]
        return Self(PathBuf::from("hpatchz.exe"));
    }
}

impl From<PathBuf> for HdiffPatcher {
    #[inline]
    fn from(path: PathBuf) -> Self {
        Self(path)
    }
}

impl From<HdiffPatcher> for PathBuf {
    #[inline]
    fn from(patcher: HdiffPatcher) -> Self {
        patcher.0
    }
}

impl HdiffPatcher {
    /// Write bundled hpatchz binary to the temp directory and point the patcher
    /// to use it.
    #[inline(never)]
    pub async fn export() -> std::io::Result<Self> {
        // FIXME: cache this value somewhere?
        let hash = hex::encode(Md5::digest(HPATCHZ));

        #[cfg(not(target_os = "windows"))]
        let path = std::env::temp_dir()
            .join(format!("hpatchz-{hash}"));

        #[cfg(target_os = "windows")]
        let path = std::env::temp_dir()
            .join(format!("hpatchz-{hash}.exe"));

        if !path.is_file() {
            #[cfg(feature = "tracing")]
            tracing::debug!(?path, "export bundled hpatchz binary");

            tokio::fs::write(&path, HPATCHZ).await?;

            #[cfg(target_family = "unix")] {
                use std::os::unix::fs::PermissionsExt;

                tokio::fs::set_permissions(
                    &path,
                    std::fs::Permissions::from_mode(0o755)
                ).await?;
            }
        }

        Ok(Self(path))
    }

    /// Path to the `hpatchz` binary.
    #[inline]
    pub const fn path(&self) -> &PathBuf {
        &self.0
    }

    /// Apply patch to the input file and save it under the output path. If
    /// `Ok(false)` is returned, then the patch was not applied.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = tracing::Level::DEBUG, skip(self), ret)
    )]
    pub async fn patch(
        &self,
        input: Option<&Path>,
        patch: &Path,
        output: &Path
    ) -> std::io::Result<bool> {
        let output = Command::new(&self.0)
            .arg("-f")
            .arg(input.unwrap_or(&PathBuf::default()))
            .arg(patch)
            .arg(output)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await?;

        Ok(String::from_utf8_lossy(&output.stdout).contains("patch ok!"))
    }
}
