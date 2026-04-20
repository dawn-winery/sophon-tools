use std::{
    collections::HashMap,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use crate::check_file;

#[derive(Debug)]
pub(crate) struct FileCheckCache {
    cache: HashMap<(PathBuf, u64, String), (i64, bool)>,
}

impl FileCheckCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
        }
    }

    pub(crate) fn check_file(
        &mut self,
        file_path: &Path,
        expected_size: u64,
        expected_md5: &str,
    ) -> bool {
        if let Some((mtime, check_res)) = self
            .cache
            .get(&(file_path.to_owned(), expected_size, expected_md5.to_owned()))
            .copied()
        {
            if std::fs::metadata(file_path)
                .map(|m| m.mtime() > mtime)
                .unwrap_or(false)
            {
                self.cache
                    .remove(&(file_path.to_owned(), expected_size, expected_md5.to_owned()));
            } else {
                return check_res;
            }
        }
        let check_res = check_file(file_path, expected_size, expected_md5).unwrap_or(false);
        let mtime = std::fs::metadata(file_path)
            .map(|m| m.mtime())
            .unwrap_or(i64::MIN);
        self.cache.insert(
            (file_path.to_owned(), expected_size, expected_md5.to_owned()),
            (mtime, check_res),
        );
        check_res
    }
}
