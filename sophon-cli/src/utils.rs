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

use std::time::Duration;

pub fn parse_duration_str(duration: &str) -> Option<Duration> {
    const MULTIPLIERS: &[(&str, f32)] = &[
        ("hours", 60.0 * 60.0),
        ("hour",  60.0 * 60.0),
        ("h",     60.0 * 60.0),

        ("minutes", 60.0),
        ("minute",  60.0),
        ("mins",    60.0),
        ("min",     60.0),
        ("m",       60.0),

        ("secounds", 1.0),
        ("secound", 1.0),
        ("secs", 1.0),
        ("sec", 1.0),
        ("s", 1.0)
    ];

    let value = duration.trim()
        .to_lowercase();

    let mut duration = value.parse::<f32>()
        .map(Duration::from_secs_f32)
        .ok();

    if duration.is_none() {
        for (suffix, multiplier) in MULTIPLIERS {
            if let Some(prefix) = value.strip_suffix(suffix)
                && let Ok(value) = prefix.trim().parse::<f32>()
            {
                duration = Some(Duration::from_secs_f32(value * multiplier));

                break;
            }
        }
    }

    duration
}

pub fn parse_memory_str(value: &str) -> Option<u64> {
    const MULTIPLIERS: &[(&str, f64)] = &[
        ("tb", 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("t",  1024.0 * 1024.0 * 1024.0 * 1024.0),

        ("gb", 1024.0 * 1024.0 * 1024.0),
        ("g",  1024.0 * 1024.0 * 1024.0),

        ("mb", 1024.0 * 1024.0),
        ("m",  1024.0 * 1024.0),

        ("kb", 1024.0),
        ("k",  1024.0),

        ("b",  1.0)
    ];

    let value = value.trim().to_lowercase();

    let mut memory = value.parse::<u64>().ok();

    if memory.is_none() {
        for (suffix, multiplier) in MULTIPLIERS {
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
