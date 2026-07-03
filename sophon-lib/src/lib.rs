// SPDX-License-Identifier: GPL-3.0-or-later
//
// sophon-tools
// Copyright (C) 2026  Nikita Podvirnyi <krypt0nn@dawn.wine>
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

use std::time::Duration;

/// `sophon-tools` version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod export {
    pub use reqwest;
    pub use tokio;
    pub use futures;
}

pub mod region;
pub mod api;
pub mod protos;
pub mod verifier;
pub mod patcher;
pub mod downloader;
pub mod updater;

/// Get standard `sophon-tools` reqwest client builder.
pub fn client_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new()
        .user_agent(format!("sophon-tools/v{VERSION}"))
        .connect_timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(180))
        .http2_keep_alive_interval(Some(Duration::from_secs(30)))
        .http2_keep_alive_timeout(Duration::from_secs(20))
}
