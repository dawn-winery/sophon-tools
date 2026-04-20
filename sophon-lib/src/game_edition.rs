use std::str::FromStr;

use thiserror::Error;

/// Implement conversion traits into this enum for use with the APIs, used for getting launcher id,
/// api host, etc
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum GameEdition {
    Global,
    China,
    GlobalBeta { launcher_id: String },
    ChinaBeta { launcher_id: String },
}

impl GameEdition {
    #[inline]
    pub fn branches_host(&self) -> &str {
        match self {
            Self::Global => concat!("https://", "sg-hy", "p-api.", "h", "oy", "over", "se.com"),
            Self::China => concat!("https://", "hy", "p-api.", "mi", "h", "oyo", ".com"),
            Self::GlobalBeta { .. } => {
                concat!("https://", "sg-hy", "p-api-beta.", "hoy", "overse.com")
            }
            Self::ChinaBeta { .. } => {
                concat!("https://", "hy", "p-api-beta.", "mi", "h", "oyo", ".com")
            }
        }
    }

    #[inline]
    pub fn api_host(&self) -> &str {
        match self {
            Self::Global => concat!("https://", "sg-pu", "blic-api.", "hoy", "over", "se.com"),
            Self::China => concat!("https://", "api-t", "ak", "umi.", "mi", "h", "oyo", ".com"),
            Self::GlobalBeta { .. } => {
                concat!("https://", "sg-be", "ta-api.", "hoy", "over", "se.com")
            }
            Self::ChinaBeta { .. } => {
                concat!("https://", "downloader-api-beta.", "mih", "oyo", ".com")
            }
        }
    }

    #[inline]
    pub fn launcher_id(&self) -> &str {
        match self {
            Self::Global => "VYTpXlbWo8",
            Self::China => "jGHBHlcOq1",
            Self::GlobalBeta { launcher_id } => launcher_id,
            Self::ChinaBeta { launcher_id } => launcher_id,
        }
    }
}

#[derive(Debug, Error)]
#[error("Unknown game edition: {0}")]
pub struct UnknownValue(String);

impl FromStr for GameEdition {
    type Err = UnknownValue;

    /// Case-insensitive parse
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowercased = s.to_lowercase();
        match lowercased.as_str() {
            "global" => Ok(Self::Global),
            "china" => Ok(Self::China),
            "global-beta" => Ok(Self::GlobalBeta {
                launcher_id: "URRJEUzW3X".to_owned(),
            }),
            "china-beta" => Ok(Self::ChinaBeta {
                launcher_id: "GcFHm7rte6".to_owned(),
            }),
            _ if lowercased.starts_with("global-beta-") => Ok(Self::GlobalBeta {
                launcher_id: s[12..].to_owned(),
            }),
            _ if lowercased.starts_with("china-beta-") => Ok(Self::ChinaBeta {
                launcher_id: s[11..].to_owned(),
            }),
            _ => Err(UnknownValue(lowercased)),
        }
    }
}
