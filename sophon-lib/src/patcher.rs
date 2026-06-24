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
use std::process::Command;

const HPATCHZ: &[u8] = include_bytes!("../../external/hpatchz/hpatchz");

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
    pub fn export() -> std::io::Result<Self> {
        let hash = seahash::hash(HPATCHZ);

        let path = std::env::temp_dir()
            .join(format!("hpatchz-{hash:0x}"));

        if !path.is_file() {
            std::fs::write(&path, HPATCHZ)?;
        }

        Ok(Self(path))
    }

    /// Apply patch to the input file and save it under the output path.
    pub fn patch(
        &self,
        input: &Path,
        patch: &Path,
        output: &Path
    ) -> std::io::Result<()> {
        Command::new(&self.0)
            .arg(input)
            .arg(patch)
            .arg(output)
            .output()?;

        Ok(())
    }
}
