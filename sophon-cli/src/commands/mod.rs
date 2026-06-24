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

use clap::ValueEnum;

pub mod list_games;
pub mod list_components;
pub mod game_versions;
pub mod download_info;
pub mod verify_game;
pub mod download_game;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SophonRegion {
    #[value(name = "global")]
    Global,

    #[value(name = "china")]
    China
}

impl From<SophonRegion> for sophon_lib::region::SophonRegion {
    fn from(value: SophonRegion) -> Self {
        match value {
            SophonRegion::Global => Self::Global,
            SophonRegion::China => Self::China
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    #[value(name = "text")]
    Text,

    #[value(name = "json")]
    Json
}

#[derive(Default)]
pub struct ProgressBar {
    pub current: u64,
    pub total: u64,

    /// Format current and total as bytes.
    pub format_bytes: bool
}

impl nutmeg::Model for ProgressBar {
    fn render(&mut self, width: usize) -> String {
        let (current, total) = if self.format_bytes {
            (format_size(self.current as f64), format_size(self.total as f64))
        } else {
            (self.current.to_string(), self.total.to_string())
        };

        if current.len() + total.len() + 6 > width {
            return String::new();
        }

        let pb_width = width - current.len() - total.len() - 6;

        let pb_prefix_width = (self.current as f64 * pb_width as f64 / self.total as f64).round() as usize;
        let pb_suffix_width = pb_width - pb_prefix_width;

        let pb_prefix = "#".repeat(pb_prefix_width);
        let pb_suffix = " ".repeat(pb_suffix_width);

        format!("{current} / {total} [{pb_prefix}{pb_suffix}]")
    }
}

/// Format bytes string.
pub fn format_size(size: f64) -> String {
    if size > 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB", size / 1024.0 / 1024.0 / 1024.0)
    } else if size > 1024.0 * 1024.0 {
        format!("{:.2} MB", size / 1024.0 / 1024.0)
    } else if size > 1024.0 {
        format!("{:.2} KB", size / 1024.0)
    } else {
        format!("{} B", size)
    }
}
