use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
    num::ParseIntError,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl Version {
    #[inline]
    pub fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Converts `Version` struct to plain format (e.g. "123")
    ///
    /// ```
    /// # use sophon_lib::utils::version::Version;
    ///
    /// assert_eq!(Version::new(1, 2, 3).to_plain_string(), "123");
    /// ```
    pub fn to_plain_string(&self) -> String {
        format!("{}{}{}", self.major, self.minor, self.patch)
    }
}

// Conversion

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VersionParseError {
    #[error("Invalid number of dot-delimeted segments, expected 3, got {got}")]
    InvalidNumberAmount { got: u8 },
    #[error("Failed to parse one of teh version numbers: {0}")]
    ParseIntError(#[from] ParseIntError),
}

impl FromStr for Version {
    type Err = VersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.').collect::<Vec<&str>>();

        if parts.len() != 3 {
            return Err(VersionParseError::InvalidNumberAmount {
                got: parts.len() as u8,
            });
        }

        Ok(Version::new(
            parts[0].parse()?,
            parts[1].parse()?,
            parts[2].parse()?,
        ))
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

// Equality with strings

impl PartialEq<String> for Version {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        &self.to_string() == other
    }
}

impl PartialEq<Version> for String {
    #[inline]
    fn eq(&self, other: &Version) -> bool {
        self == &other.to_string()
    }
}

impl PartialEq<&str> for Version {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        &self.to_string() == other
    }
}

impl PartialEq<Version> for &str {
    #[inline]
    fn eq(&self, other: &Version) -> bool {
        self == &other.to_string()
    }
}

// Comparison with strings

impl PartialOrd<String> for Version {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        self.to_string().partial_cmp(other)
    }
}

impl PartialOrd<Version> for String {
    fn partial_cmp(&self, other: &Version) -> Option<Ordering> {
        self.partial_cmp(&other.to_string())
    }
}

impl PartialOrd<&str> for Version {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        self.to_string().as_str().partial_cmp(*other)
    }
}

impl PartialOrd<Version> for &str {
    fn partial_cmp(&self, other: &Version) -> Option<Ordering> {
        self.partial_cmp(&other.to_string().as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_version_new() {
        let version = Version::new(0, 0, 0);

        assert_eq!(version, "0.0.0");
        assert_eq!(version, "0.0.0".to_string());
        assert_eq!(Ok(version), Version::from_str("0.0.0"));
        assert_eq!(version.to_plain_string(), "000".to_string());
    }

    #[test]
    fn test_version_from_str() {
        let version = Version::from_str("0.0.0");

        assert!(version.is_ok());

        let version = version.unwrap();

        assert_eq!(version, "0.0.0");
        assert_eq!(version, "0.0.0".to_string());
        assert_eq!(version, Version::new(0, 0, 0));
        assert_eq!(version.to_plain_string(), "000".to_string());
    }

    #[test]
    fn test_version_long() {
        let version = Version::from_str("100.0.255");

        assert!(version.is_ok());

        let version = version.unwrap();

        assert_eq!(version, "100.0.255");
        assert_eq!(version, "100.0.255".to_string());
        assert_eq!(version, Version::new(100, 0, 255));
        assert_eq!(version.to_plain_string(), "1000255".to_string());
    }

    #[test]
    fn test_incorrect_versions() {
        assert!(Version::from_str("").is_err());
        assert!(Version::from_str("..0").is_err());
        assert!(Version::from_str("0.0.").is_err());
    }

    #[test]
    #[allow(clippy::cmp_owned)]
    fn test_version_comparison() {
        assert!(Version::new(1, 0, 1) > "1.0.0");
        assert!(Version::new(1, 0, 0) < "1.0.1");

        assert!("1.0.0" < Version::new(1, 0, 1));
        assert!("1.0.1" > Version::new(1, 0, 0));

        assert!(Version::new(1, 0, 1) > String::from("1.0.0"));
        assert!(Version::new(1, 0, 0) < String::from("1.0.1"));

        assert!(String::from("1.0.0") < Version::new(1, 0, 1));
        assert!(String::from("1.0.1") > Version::new(1, 0, 0));

        assert!(Version::new(1, 0, 0) == "1.0.0");
        assert!("1.0.0" == Version::new(1, 0, 0));

        assert!(Version::new(1, 0, 0) == String::from("1.0.0"));
        assert!(String::from("1.0.0") == Version::new(1, 0, 0));
    }
}
