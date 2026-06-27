#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Hash,
    serde::Serialize, serde::Deserialize
)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8
}

impl Version {
    #[inline]
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VersionParseError {
    #[error("version string has invalid format")]
    InvalidFormat,

    #[error("failed to parse version string part as a number: {0}")]
    ParseIntError(#[from] std::num::ParseIntError)
}

impl std::str::FromStr for Version {
    type Err = VersionParseError;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        let mut parts = [0; 3];

        for (i, part) in version.split('.').enumerate() {
            match parts.get_mut(i) {
                Some(value) => *value = part.parse::<u8>()?,
                None => return Err(VersionParseError::InvalidFormat)
            }
        }

        Ok(Self {
            major: parts[0],
            minor: parts[1],
            patch: parts[2]
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}
