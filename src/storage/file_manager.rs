#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::process::exit;
use std::str;
use std::sync::Mutex;

use crate::storage::block_id::BlockId;
use crate::storage::page::Page;

///
/// FileMgr.
///
/// Represents the main interface to store and load data from file system into Page and back.
///
struct FileManager {
    db_dir: String,
    files_map: Mutex<HashMap<String, File>>,
    block_size: u64,
}

impl FileManager {
    pub fn new(db_dir: String, block_size: u64) -> Self {
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
            db_dir,
            files_map: Mutex::new(HashMap::new()),
            block_size,
        }
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
                .open(&full_file_path)
                .expect(&format!("Can't open file at: '{:?}'", &full_file_path));

            // files_map
            //     .insert(
            //         file_name.to_string().clone(),
            //         file.try_clone().expect("Can't clone File"),
            //     )
            //     .expect("Insert failed");

            return file;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_to_file_and_read() {
        let mut file_mgr =
            FileManager::new("/Users/mstepan/repo-rust/simpledb/db".to_string(), 4096);

        let mut page = Page::new(4096);
        page.put_string(100, "user: 123, age: 99");
        page.put_string(200, "Writing you own DB engine is complicated");

        let block = BlockId::new("user.data".to_string(), 0);

        file_mgr.write_to_file(&page, &block);

        let mut page_from_file = file_mgr.read_from_file(&block);

        assert_eq!("user: 123, age: 99", page_from_file.get_string(100));
        assert_eq!(
            "Writing you own DB engine is complicated",
            page_from_file.get_string(200)
        );
    }
}
