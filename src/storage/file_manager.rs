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
    pub fn with_default_block_size(db_dir: &str) -> Self {
        return Self::new(db_dir, DEFAULT_BLOCK_SIZE);
    }

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
    pub fn length(&self, file_name: &str) -> u64 {
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
    pub fn write_to_file(&mut self, page: &Page, block: &BlockId) {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'write_to_file'");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, &block.file_name);

        file.write_all_at(&page.data, block.block_no * self.block_size)
            .expect("Can;t write page to file");
    }

    ///
    /// Read block from file into in-memory Page
    ///
    pub fn read_from_file(&mut self, block: &BlockId) -> Page {
        let mut files_map = self
            .files_map
            .lock()
            .expect("'files_map' lock failed during 'read_from_file'");

        let file = Self::get_file_from_map(&self.db_dir, &mut files_map, &block.file_name);

        let mut page = Page::new(self.block_size);

        file.read_exact_at(&mut page.data, block.block_no * self.block_size)
            .expect("Can't read page from file");

        return page;
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
    use std::fs::{metadata, remove_dir_all};
    use std::panic;

    const DB_DIR_TEST: &str = "/Users/mstepan/repo-rust/simpledb/db-test";

    static LOCK: Mutex<i32> = Mutex::new(0);

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce() -> () + panic::UnwindSafe,
    {
        // Allow only one test at a time, otherwise we will have spurious issues b/c
        // the same 'DB_DIR_TEST' directory is used
        let _guard = LOCK.lock().unwrap();
        setup();
        let result = panic::catch_unwind(|| test());
        teardown();
        assert!(result.is_ok())
    }

    fn setup() {
        if metadata(DB_DIR_TEST).is_ok() {
            remove_dir_all(DB_DIR_TEST).expect("Can't delete 'DB_DIR_TEST' in setup");
        }

        create_dir_all(DB_DIR_TEST).expect("Can't create 'DB_DIR_TEST' in setup")
    }

    fn teardown() {
        remove_dir_all(DB_DIR_TEST).expect("Can't delete 'DB_DIR_TEST' in teardown")
    }

    #[test]
    fn create_with_default_capacity() {
        run_test(|| {
            let file_mgr = FileManager::with_default_block_size(DB_DIR_TEST);

            assert_eq!(DB_DIR_TEST, file_mgr.db_dir);
            assert_eq!(DEFAULT_BLOCK_SIZE, file_mgr.block_size());
        });
    }

    #[test]
    #[should_panic]
    fn create_with_zero_block_size_should_panic() {
        FileManager::new(DB_DIR_TEST, 0);
    }

    #[test]
    fn append() {
        run_test(|| {
            let mut file_mgr = FileManager::new(DB_DIR_TEST, DEFAULT_BLOCK_SIZE);

            let appends_count = 3;

            for _ in 0..appends_count {
                file_mgr.append("log.dat");
            }

            let file =
                File::open(format!("{}/log.dat", DB_DIR_TEST)).expect("Can't open 'log.dat' file");

            assert_eq!(
                appends_count * DEFAULT_BLOCK_SIZE,
                file.metadata().unwrap().len(),
            );
        });
    }

    #[test]
    fn write_to_file_and_read() {
        run_test(|| {
            let mut file_mgr = FileManager::new(DB_DIR_TEST, DEFAULT_BLOCK_SIZE);

            let mut page = Page::new(DEFAULT_BLOCK_SIZE);
            page.put_string(100, "user: 123, age: 99");
            page.put_string(200, "Writing you own DB engine is complicated");

            let block = BlockId::new("user.data".to_string(), 0);

            // write to file 1st time
            file_mgr.write_to_file(&page, &block);

            // read from file
            let mut page_from_file1 = file_mgr.read_from_file(&block);

            assert_eq!("user: 123, age: 99", page_from_file1.get_string(100));
            assert_eq!(
                "Writing you own DB engine is complicated",
                page_from_file1.get_string(200)
            );

            let mut new_page = Page::new(DEFAULT_BLOCK_SIZE);
            new_page.put_string(100, "some new data");

            // write to file 2nd time
            file_mgr.write_to_file(&new_page, &block);

            let mut page_from_file2 = file_mgr.read_from_file(&block);

            assert_eq!("some new data", page_from_file2.get_string(100));
        });
    }
}
