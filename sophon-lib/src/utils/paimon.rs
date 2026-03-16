//! Utility wrappers to interact with [`paimon`], the hdiff parser and applier written in rust

use std::{
    fs::File,
    io::{BufReader, BufWriter, Cursor, Read, SeekFrom, Write},
    path::Path,
};

use paimon::diffs::hdiff13::HDiff13;

use crate::{
    updater::{PatchFnArgs, PatchLocation},
    utils::read_take_region::ReadTakeRegion,
};

/// A wrapper around [`paimon_parse_apply`] that just handles the [`PatchLocation`] variants
pub fn paimon_patch(args: PatchFnArgs<'_>) -> std::io::Result<()> {
    match args.patch {
        PatchLocation::Filesystem(patch_path) => {
            let mut patch_file = BufReader::new(File::open(patch_path)?);
            paimon_parse_apply(&mut patch_file, args.src_file, args.out_file)
        }
        PatchLocation::Memory(data) => {
            let mut data_cursor = Cursor::new(data);
            paimon_parse_apply(&mut data_cursor, args.src_file, args.out_file)
        }
        PatchLocation::FilesystemRegion {
            combined_path,
            offset,
            length,
        } => {
            let mut patch_file_region = BufReader::new(
                File::open(combined_path)?.take_region(SeekFrom::Start(*offset), *length)?,
            );
            paimon_parse_apply(&mut patch_file_region, args.src_file, args.out_file)
        }
    }
}

/// Parse the patch from the provided reader and use the file paths to apply the patch
pub fn paimon_parse_apply<R>(
    patch_reader: &mut R,
    src_file: &Path,
    out_file: &Path,
) -> std::io::Result<()>
where
    R: Read,
{
    let mut hdiff_parsed = HDiff13::parse(patch_reader).map_err(std::io::Error::other)?;
    let mut src_file = BufReader::new(File::open(src_file)?);
    let mut out_file = BufWriter::new(File::create(out_file)?);
    hdiff_parsed
        .apply(&mut src_file, &mut out_file)
        .map_err(std::io::Error::other)?;
    out_file.flush()?;
    Ok(())
}
