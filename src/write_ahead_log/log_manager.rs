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

        // store_pos can be negative here
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

        self.page
            .put_bytes(store_pos as usize, data)
            .expect("PageOverflow occurred");

        self.cur_lsn += 1;
        return self.cur_lsn;
    }
}

impl<'a> IntoIterator for LogManager<'a> {
    type Item = Vec<u8>;
    type IntoIter = LogIterator<'a>;
    fn into_iter(self) -> LogIterator<'a> {

        let blocks_count = self
            .file_mgr
            .length_in_logical_blocks(&self.log_file_name.clone());

        let block = BlockId::new(self.log_file_name.clone(), blocks_count-1);
        let mut page = Page::new(self.file_mgr.block_size());


        self.file_mgr.load_page(&block, &mut page);
        let record_pos = self.page.get_u64(0);

        return LogIterator {
            file_mgr: self.file_mgr,
            log_file_name: self.log_file_name.clone(),
            page,
            record_pos,
            block,
        };
    }
}

///
/// Iterates over LogManager logs from the recent one to the oldest one.
///
pub struct LogIterator<'a> {
    file_mgr: &'a mut FileManager,
    log_file_name: String,
    page: Page,
    record_pos: u64,
    block: BlockId
}

impl LogIterator<'_> {
    fn move_to_next_block(&mut self) {
        self.block = BlockId::new(self.log_file_name.clone(), self.block.block_no - 1);
        self.page = Page::new(self.file_mgr.block_size());

        self.file_mgr.load_page(&self.block, &mut self.page);
        self.record_pos = self.page.get_u64(0);
    }
}

impl Iterator for LogIterator<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.record_pos == self.file_mgr.block_size() {

            // 0-block reached, nothing left to traverse
            if self.block.block_no  == 0 {
                return None;
            }
            else {
                self.move_to_next_block();
            }
        }

        let log_entry = self.page.get_bytes(self.record_pos as usize);

        self.record_pos += (log_entry.len() + INTEGER_SIZE_IN_BYTES) as u64;

        return Some(log_entry.to_vec());
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
            let log_mgr = LogManager::new(&mut file_mgr, "log-file.data");

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
            let mut log_mgr = LogManager::new(&mut file_mgr, "log-file.data");

            let lsn = log_mgr.append("message-1".as_bytes());
            assert_eq!(1, lsn);

            log_mgr.append("message-2".as_bytes());
            let lsn = log_mgr.append("message-3".as_bytes());
            assert_eq!(3, lsn);
        });
    }

    #[test]
    fn append_logs_then_iterate() {
        let db_dir_test = temp_dir()
            .join("simpledb/log-manager")
            .to_str()
            .unwrap()
            .to_string();

        let mut test_util = FSTestUtil::new(&db_dir_test);
        test_util.run_test(|dir| {
            let mut file_mgr = FileManager::new(dir, 64);
            let mut log_mgr = LogManager::new(&mut file_mgr, "log-file.data");

            for i in 0..10 {
                let msg = format!("message-{}", i);
                log_mgr.append(msg.as_bytes());
            }
            log_mgr.flush_force();

            let mut it = log_mgr.into_iter();

            for i in (0..10).rev() {
                assert_eq!(
                    format!("message-{}", i),
                    String::from_utf8(it.next().unwrap()).unwrap(),
                );
            }
        });
    }
}
