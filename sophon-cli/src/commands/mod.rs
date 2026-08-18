// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@dawn.wine>
//                     "John the Cooling Fan" <ivan8215145640@gmail.com>
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

pub mod list_games;
pub mod list_components;
pub mod game_versions;
pub mod download_info;

pub mod detect_game;
pub mod verify_game;
pub mod download_game;
pub mod update_game;

#[derive(Default)]
pub struct ProgressBar {
    pub current: u64,
    pub total: u64,

    /// Prefix message.
    pub prefix: String,

    /// Format current and total as bytes.
    pub format_bytes: bool
}

impl nutmeg::Model for ProgressBar {
    fn render(&mut self, width: usize) -> String {
        if width < 7 {
            return String::new();
        }

        let (current, total) = if self.format_bytes {
            (format_size(self.current as f64), format_size(self.total as f64))
        } else {
            (self.current.to_string(), self.total.to_string())
        };

        if current.len() + total.len() + self.prefix.len() + 7 > width {
            return String::new();
        }

        let mut pb_width = width - current.len() - total.len() - 6;

        if !self.prefix.is_empty() {
            pb_width -= self.prefix.len() + 1;
        }

        let pb_prefix_width = (self.current as f64 * pb_width as f64 / self.total as f64).round() as usize;
        let pb_suffix_width = pb_width.saturating_sub(pb_prefix_width);

        let pb_prefix = "#".repeat(pb_prefix_width);
        let pb_suffix = " ".repeat(pb_suffix_width);

        if self.prefix.is_empty() {
            format!("{current} / {total} [{pb_prefix}{pb_suffix}]")
        } else {
            format!("{} {current} / {total} [{pb_prefix}{pb_suffix}]", self.prefix)
        }
    }
}

pub fn find_game_name(id: &str, biz: &str) -> Option<&'static str> {
    const GAMES: &[(&str, &str, &str)] = &[
        // Global
        ("gopR6Cufr3", "hk4e_global",  "Genshin Impact"),
        ("U5hbdsT9W7", "nap_global",   "Zenless Zone Zero"),
        ("4ziysqXOQ8", "hkrpg_global", "Honkai: Star Rail"),
        ("5TIVvvcwtM", "bh3_global",   "Honkai Impact 3rd"),
        ("g0mMIvshDb", "bh3_global",   "Honkai Impact 3rd"),
        ("uxB4MC7nzC", "bh3_global",   "Honkai Impact 3rd"),
        ("bxPTXSET5t", "bh3_global",   "Honkai Impact 3rd"),
        ("wkE5P5WsIf", "bh3_global",   "Honkai Impact 3rd"),

        // China
        ("1Z8W5NHUQb", "hk4e_cn",  "Genshin Impact"),
        ("x6znKlJ0xK", "nap_cn",   "Zenless Zone Zero"),
        ("64kMb5iAWu", "hkrpg_cn", "Honkai: Star Rail"),
        ("osvnlOc0S8", "bh3_cn",   "Honkai Impact 3rd")
    ];

    for (game_id, game_biz, game_name) in GAMES.iter().copied() {
        if game_id == id || game_biz == biz {
            return Some(game_name);
        }
    }

    None
}

pub fn find_component_title(name: &str) -> Option<&'static str> {
    const COMPONENTS: &[(&str, &str)] = &[
        ("game",  "Base game"),
        ("en-us", "English voiceover"),
        ("zh-cn", "Chinese voiceover"),
        ("ja-jp", "Japanese voiceover"),
        ("ko-kr", "Korean voiceover")
    ];

    for (component_name, component_title) in COMPONENTS.iter().copied() {
        if component_name == name {
            return Some(component_title);
        }
    }

    None
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
