//! Utilities for independently handling newfiles that are just HDIFFs but don't actually need much
//! parsing

use std::{
    fs::File,
    io::{Cursor, Read, Seek},
    path::Path,
};

/// Attempt to extract newfile from the hdiff, returning:
/// - `Ok(Some(()))` if the file is an hdiff
/// - `Ok(None)` if the file is not an hdiff
/// - `Err` if any other errors occur in the process
pub fn try_new_file_hdiff<R>(
    file_length: u64,
    hdiff_file: &mut R,
    tmp_path: &Path,
) -> std::io::Result<Option<()>>
where
    R: Read,
    R: Seek,
{
    parse_hdiff(hdiff_file)
        .ok()
        .map(|(is_compressed, inner_size)| {
            new_file_hdiff(
                is_compressed,
                file_length - inner_size,
                inner_size,
                hdiff_file,
                tmp_path,
            )
        })
        .transpose()
}

/// Extract the newfile from an hdiff (which was previously parsed via [`parse_hdiff`])
pub fn new_file_hdiff<R>(
    is_compressed: bool,
    offset: u64,
    inner_size: u64,
    hdiff_file: &mut R,
    tmp_path: &Path,
) -> std::io::Result<()>
where
    R: Read,
    R: Seek,
{
    tracing::debug!("Using weird hdiff workaround");
    hdiff_file.seek(std::io::SeekFrom::Start(offset))?;
    let mut out_tmp_file = File::create(tmp_path)?;
    out_tmp_file.set_len(inner_size)?;
    if is_compressed {
        let mut decoder = zstd::Decoder::new(hdiff_file)?;
        std::io::copy(&mut decoder, &mut out_tmp_file)?;
    } else {
        std::io::copy(hdiff_file, &mut out_tmp_file)?;
    }
    Ok(())
}

/// Returns whether the file is compressed and how many last bytes the file takes, or an error if
/// the file is not an hdiff or there's an issue reading the file
pub fn parse_hdiff<R: Read>(reader: &mut R) -> std::io::Result<(bool, u64)> {
    let mut buf = [0_u8; 128];
    reader.read_exact(&mut buf)?;
    let buf = buf;
    if !buf.starts_with(b"HDIFF13") {
        return Err(std::io::Error::other(
            "Invalid HDIFF, file does not start with HDIFF13",
        ));
    }
    let header_start = buf
        .iter()
        .enumerate()
        .find(|(_, b)| **b == 0)
        .ok_or(std::io::Error::other("Invalid HDIFF, 0x0 not found"))?
        .0 as u64;
    let mut operating_buf = Cursor::new(&buf);
    operating_buf.seek(std::io::SeekFrom::Start(header_start))?;
    (0..10).for_each(|_| {
        let _ = read_varint(&mut operating_buf);
    });
    //eprintln!("{}", operating_buf.stream_position().unwrap());
    let new_data_diff_size = read_varint(&mut operating_buf)?;
    let compressed_new_data_diff_size = read_varint(&mut operating_buf)?;
    if compressed_new_data_diff_size == 0 {
        Ok((false, new_data_diff_size))
    } else {
        Ok((true, compressed_new_data_diff_size))
    }
}

fn read_varint<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    const CONTINUE_BIT: u8 = 0b10000000;
    const MASK_BOTTOM: u8 = !CONTINUE_BIT;

    let mut byte = read_u8(reader)?;
    let mut res = (byte & MASK_BOTTOM) as u64;
    while byte & CONTINUE_BIT != 0 {
        byte = read_u8(reader)?;
        res <<= 7;
        res |= (byte & MASK_BOTTOM) as u64;
    }

    Ok(res)
}

fn read_u8<R: Read>(reader: &mut R) -> std::io::Result<u8> {
    let mut buf = [0_u8];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}
