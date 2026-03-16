//! Utility for getting a [`Read`]er of a specific region

use std::io::{Read, Seek, SeekFrom, Take};

/// Utility trait for [`Seek::seek`] + [`Read::take`]
pub trait ReadTakeRegion: Seek + Read + Sized {
    fn take_region(mut self, seek: SeekFrom, take: u64) -> std::io::Result<Take<Self>> {
        self.seek(seek)?;
        Ok(self.take(take))
    }
}

impl<T: Sized> ReadTakeRegion for T
where
    T: Read,
    T: Seek,
{
}
