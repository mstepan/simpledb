#![allow(dead_code)]
use crate::storage::block_id::BlockId;
use crate::storage::file_manager::FileManager;
use crate::storage::page::Page;
use crate::utils::primitive_types::{INTEGER_SIZE_IN_BYTES, LONG_SIZE_IN_BYTES};

///
/// The main purpose of LogManager is to manage APPEND-only list of logs.
///
pub struct LogManager<'a> {
    file_mgr: &'a mut FileManager,
    log_file_name: String,
    cur_lsn: u32,
    last_saved_lsn: u32,
    cur_block: BlockId,
    page: Page,
}

impl<'a> LogManager<'a> {
    ///
    /// Create new LogManager. There should be ONE log manager per DB.
    ///
    pub fn new(file_mgr: &'a mut FileManager, log_file_name: &str) -> Self {
        let mut page = Page::new(file_mgr.block_size());

        let log_file_size_in_blocks = file_mgr.length_in_logical_blocks(log_file_name);

        let cur_block;

        if log_file_size_in_blocks == 0 {
            cur_block = Self::append_new_block(file_mgr, log_file_name, &mut page);
        } else {
            cur_block = BlockId::new(log_file_name.to_string(), log_file_size_in_blocks - 1);
            file_mgr.load_page(&cur_block, &mut page);
        };

        return Self {
            file_mgr,
            log_file_name: log_file_name.to_string(),
            cur_lsn: 0,
            last_saved_lsn: 0,
            cur_block,
            page,
        };
    }

    fn append_new_block(
        file_mgr: &mut FileManager,
        log_file_name: &str,
        page: &mut Page,
    ) -> BlockId {
        let cur_block = file_mgr.append(log_file_name);
        page.put_u64(0, file_mgr.block_size());
        file_mgr.store_page(&cur_block, &page);
        return cur_block;
    }

    ///
    /// Save log page into file system if any changes detected.
    ///
    pub fn flush(&mut self, lsn: u32) {
        if lsn > self.last_saved_lsn {
            self.flush_force();
        }
    }

    ///
    /// Private method to flush changes to file system.
    ///
    fn flush_force(&mut self) {
        self.file_mgr.store_page(&self.cur_block, &self.page);
        self.last_saved_lsn = self.cur_lsn;
    }

    ///
    /// Append log information. The log information is saved right-to-left, so that
    /// LogIterator can read from most recent value to the oldest one in left-to-right order.
    ///
    pub fn append(&mut self, data: &[u8]) -> u32 {
        let mut boundary = self.page.get_u64(0);

        let data_size_in_bytes = data.len() + INTEGER_SIZE_IN_BYTES;

        //
        // value can be negative here, so will fail
        //
        let mut store_pos = (boundary as i32) - data_size_in_bytes as i32;

        // Page overflow, so store current page and append new block
        if store_pos < LONG_SIZE_IN_BYTES as i32 {
            self.file_mgr.store_page(&self.cur_block, &self.page);

            self.page = Page::new(self.file_mgr.block_size());
            self.cur_block =
                Self::append_new_block(self.file_mgr, &self.log_file_name, &mut self.page);

            // recalculate store position
            boundary = self.page.get_u64(0);
            store_pos = (boundary as i32) - data_size_in_bytes as i32;
        }

        self.page.put_u64(0, store_pos as u64);
        self.page.put_bytes(store_pos as usize, data);

        self.cur_lsn += 1;
        return self.cur_lsn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::fs_test_utils::FSTestUtil;
    use std::env::temp_dir;

    #[test]
    fn create_log_manager() {
        let db_dir_test = temp_dir()
            .join("simpledb/log-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let mut file_mgr = FileManager::with_default_block_size(dir);
            let log_mgr = LogManager::new(&mut file_mgr, "log-file.dat");

            assert_eq!(0, log_mgr.cur_lsn);
            assert_eq!(0, log_mgr.last_saved_lsn);
        });
    }

    #[test]
    fn append_logs() {
        let db_dir_test = temp_dir()
            .join("simpledb/log-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let mut file_mgr = FileManager::with_default_block_size(dir);
            let mut log_mgr = LogManager::new(&mut file_mgr, "log-file.dat");

            let lsn = log_mgr.append("message-1".as_bytes());
            assert_eq!(1, lsn);

            log_mgr.append("message-2".as_bytes());
            let lsn = log_mgr.append("message-3".as_bytes());
            assert_eq!(3, lsn);
        });
    }
}
