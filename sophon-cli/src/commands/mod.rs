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

use sophon_lib::export::reqwest::{
    ClientBuilder as ReqwestClientBuilder,
    Proxy as ReqwestProxy
};

pub mod list_games;
pub mod list_components;
pub mod game_versions;
pub mod download_info;

pub mod detect_game;
pub mod verify_game;
pub mod download_game;
pub mod update_game;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum SophonRegion {
    #[value(name = "global")]
    Global,

    #[value(name = "china")]
    China
}

impl From<SophonRegion> for sophon_lib::region::SophonRegion {
    fn from(region: SophonRegion) -> Self {
        match region {
            SophonRegion::Global => Self::Global,
            SophonRegion::China => Self::China
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum VerifyMethod {
    #[value(
        name = "none",
        alias = "disable",
        alias = "disabled"
    )]
    None,

    #[value(
        name = "fast",
        alias = "size",
        alias = "sizes",
        alias = "file-size",
        alias = "files-sizes"
    )]
    Fast,

    #[value(
        name = "full",
        alias = "hash",
        alias = "hashes",
        alias = "file-hash",
        alias = "files-hashes"
    )]
    Full
}

impl std::fmt::Display for VerifyMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Fast => f.write_str("fast"),
            Self::Full => f.write_str("full")
        }
    }
}

impl From<VerifyMethod> for sophon_lib::downloader::SophonDownloaderVerifyMethod {
    fn from(method: VerifyMethod) -> Self {
        match method {
            VerifyMethod::None => Self::None,
            VerifyMethod::Fast => Self::Fast,
            VerifyMethod::Full => Self::Full
        }
    }
}

impl From<VerifyMethod> for sophon_lib::updater::SophonUpdaterVerifyMethod {
    fn from(method: VerifyMethod) -> Self {
        match method {
            VerifyMethod::None => Self::None,
            VerifyMethod::Fast => Self::Fast,
            VerifyMethod::Full => Self::Full
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
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

const MEMORY_MULTIPLIERS: &[(&str, f64)] = &[
    ("tb", 1024.0 * 1024.0 * 1024.0),
    ("t",  1024.0 * 1024.0 * 1024.0),
    ("gb", 1024.0 * 1024.0 * 1024.0),
    ("g",  1024.0 * 1024.0 * 1024.0),
    ("mb", 1024.0 * 1024.0),
    ("m",  1024.0 * 1024.0),
    ("kb", 1024.0),
    ("k",  1024.0),
    ("b", 1.0)
];

pub fn parse_memory_str(value: &str) -> Option<u64> {
    let value = value.to_lowercase();

    let mut memory = value.parse::<u64>().ok();

    if memory.is_none() {
        for (suffix, multiplier) in MEMORY_MULTIPLIERS {
            if let Some(prefix) = value.strip_suffix(suffix)
                && let Ok(value) = prefix.trim().parse::<f64>()
            {
                memory = Some((value * multiplier).round() as u64);

                break;
            }
        }
    }

    memory
}

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

pub fn find_game_name(id: &str, biz: &str) -> Option<&'static str> {
    for (game_id, game_biz, game_name) in GAMES.iter().copied() {
        if game_id == id || game_biz == biz {
            return Some(game_name);
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

pub fn reqwest_client(
    user_agent: Option<String>,
    proxy: Option<String>
) -> anyhow::Result<ReqwestClientBuilder> {
    let mut builder = ReqwestClientBuilder::new()
        .user_agent(format!("sophon-tools/v{}", sophon_lib::VERSION));

    if let Some(user_agent) = user_agent {
        builder = builder.user_agent(user_agent);
    }

    if let Some(proxy) = proxy {
        builder = builder.proxy(ReqwestProxy::all(proxy)?);
    }

    Ok(builder)
}
