#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::process::exit;
use std::str;
use std::sync::Mutex;

use crate::storage::block_id::BlockId;
use crate::storage::page::Page;

#[allow(unused_imports)]
use crate::utils::fs_test_utils::FSTestUtil;

pub const DEFAULT_BLOCK_SIZE: u64 = 4096;

///
/// FileMgr.
///
/// Represents the main interface to store and load data from file system into Page and back.
///
pub struct FileManager {
    db_dir: String,
    files_map: Mutex<HashMap<String, File>>,
    block_size: u64,
}

impl FileManager {
    ///
    /// Create FileManager with specified dir and default capacity.
    ///
    pub fn with_default_block_size(db_dir: &str) -> Self {
        return Self::new(db_dir, DEFAULT_BLOCK_SIZE);
    }

    ///
    /// Create FileManager with specified dir and caapcity.
    ///
    pub fn new(db_dir: &str, block_size: u64) -> Self {
        if block_size == 0 {
            panic!("Can't create FileManager with 0 block_size");
        }

        let db_dir_path = Path::new(&db_dir);

        if db_dir_path.exists() {
            if !db_dir_path.is_dir() {
                eprintln!("'db_dir' is not a folder {:?}", db_dir_path);
                exit(-1);
            }
            println!("'db_dir' already exist: {:?}", db_dir_path);
        } else {
            println!("Creating 'db_dir': {:?}", db_dir_path);
            create_dir_all(&db_dir)
                .expect(&format!("Can't create 'db_dir' folder {:?}", db_dir_path));
        }

        Self {
            db_dir: db_dir.to_string(),
            files_map: Mutex::new(HashMap::new()),
            block_size,
        }
    }
    pub fn block_size(&self) -> u64 {
        return self.block_size;
    }

    ///
    /// Calculate file length in logical blocks. To do so we just divide real file length
    /// by the number of blocks.
    ///
    pub fn length_in_logical_blocks(&self, file_name: &str) -> u64 {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'length' call");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, file_name);

        return file
            .metadata()
            .expect("Can't read file length from metadata")
            .len()
            / self.block_size();
    }

    ///
    /// Write in-memory page into file block.
    ///
    pub fn store_page(&mut self, block: &BlockId, page: &Page) {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'write_to_file'");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, &block.file_name);

        file.write_all_at(&page.data, block.block_no * self.block_size)
            .expect("Can't write page to file");
    }

    ///
    /// Read block from file into in-memory Page
    ///
    pub fn load_page(&mut self, block: &BlockId, page: &mut Page) {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'read_from_file'");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, &block.file_name);

        // let mut page = Page::new(self.block_size);

        file.read_exact_at(&mut page.data, block.block_no * self.block_size)
            .expect("Can't read page from file");
    }

    ///
    /// Append new block to the end of a file.
    ///
    pub fn append(&mut self, file_name: &str) -> BlockId {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'read_from_file'");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, &file_name);

        let new_block_no =
            file.metadata().expect("Can't get file metadata").len() / self.block_size;
        let new_block = BlockId::new(file_name.to_string(), new_block_no);

        let mut buf = vec![0; self.block_size as usize];

        file.write_all_at(&mut buf[..], new_block.block_no * self.block_size)
            .expect("Can't append to file end");

        return new_block;
    }

    fn get_file_from_map(
        db_dir: &str,
        files_map: &mut HashMap<String, File>,
        file_name: &str,
    ) -> File {
        if files_map.contains_key(file_name) {
            return files_map
                .get(file_name)
                .expect(&format!("Can't get file '{}' from HashMap", file_name))
                .try_clone()
                .unwrap();
        } else {
            let full_file_path = Path::new(db_dir).join(&file_name);

            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                // 'O_SYNC' flag will sync all changes immediately to file system without delays
                .custom_flags(libc::O_SYNC)
                .open(&full_file_path)
                .expect(&format!("Can't open file at: '{:?}'", &full_file_path));

            files_map.insert(
                file_name.to_string().clone(),
                file.try_clone().expect("Can't clone File"),
            );

            return file;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    #[test]
    fn create_with_default_capacity() {
        let db_dir_test = temp_dir()
            .join("simpledb/file-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let file_mgr = FileManager::with_default_block_size(dir);

            assert_eq!(dir, file_mgr.db_dir);
            assert_eq!(DEFAULT_BLOCK_SIZE, file_mgr.block_size());
        });
    }

    #[test]
    #[should_panic]
    fn create_with_zero_block_size_should_panic() {
        let db_dir_test = temp_dir()
            .join("simpledb/file-manager")
            .to_str()
            .unwrap()
            .to_string();
        FileManager::new(&db_dir_test, 0);
    }

    #[test]
    fn append() {
        let db_dir_test = temp_dir()
            .join("simpledb/file-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let mut file_mgr = FileManager::new(dir, DEFAULT_BLOCK_SIZE);

            let appends_count = 3;

            for _ in 0..appends_count {
                file_mgr.append("log.dat");
            }

            let file = File::open(format!("{}/log.dat", dir)).expect("Can't open 'log.dat' file");

            assert_eq!(
                appends_count * DEFAULT_BLOCK_SIZE,
                file.metadata().unwrap().len(),
            );
        });
    }

    #[test]
    fn write_to_file_and_read() {
        let db_dir_test = temp_dir()
            .join("simpledb/file-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let mut file_mgr = FileManager::new(dir, DEFAULT_BLOCK_SIZE);

            let mut page = Page::new(DEFAULT_BLOCK_SIZE);
            page.put_string(100, "user: 123, age: 99");
            page.put_string(200, "Writing you own DB engine is complicated");

            let block = BlockId::new("user.data".to_string(), 0);

            // write to file 1st time
            file_mgr.store_page(&block, &page);

            // read from file
            let mut page_from_file1 = Page::new(file_mgr.block_size);
            file_mgr.load_page(&block, &mut page_from_file1);

            assert_eq!("user: 123, age: 99", page_from_file1.get_string(100));
            assert_eq!(
                "Writing you own DB engine is complicated",
                page_from_file1.get_string(200)
            );

            let mut new_page = Page::new(DEFAULT_BLOCK_SIZE);
            new_page.put_string(100, "some new data");

            // write to file 2nd time
            file_mgr.store_page(&block, &new_page);

            let mut page_from_file2 = Page::new(file_mgr.block_size);
            file_mgr.load_page(&block, &mut page_from_file2);

            assert_eq!("some new data", page_from_file2.get_string(100));
        });
    }
}
