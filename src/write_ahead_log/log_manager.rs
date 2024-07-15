#![allow(dead_code)]
use crate::storage::block_id::BlockId;
use crate::storage::file_manager::FileManager;
use crate::storage::page::Page;

struct LogManager<'a> {
    file_mgr: &'a FileManager,
    log_file_name: String,
    cur_lsn: u32,
    last_saved_lsn: u32,
    cur_block: BlockId,
    page: Page,
}

impl<'a> LogManager<'a> {
    pub fn new(file_mgr: &'a mut FileManager, log_file_name: &str) -> Self {
        let mut page = Page::new(file_mgr.block_size());

        let log_file_size_in_blocks = file_mgr.length(log_file_name);

        let mut cur_block;

        if log_file_size_in_blocks == 0 {
            cur_block = Self::append_new_block(file_mgr, log_file_name, &mut page);
        } else {
            cur_block = BlockId::new(log_file_name.to_string(), log_file_size_in_blocks - 1);
            file_mgr.read_from_file(&cur_block, &mut page);
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
        file_mgr.write_to_file(&page, &cur_block);

        let temp = page.get_u64(0);
        println!("temp: {}", temp);

        return cur_block;
    }

    pub fn flush(&self, lsn: u32) {}

    pub fn append(&self, data: &[u8]) -> u32 {
        return self.cur_lsn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB_DIR_TEST: &str = "/Users/mstepan/repo-rust/simpledb/db-log-manager-test";

    #[test]
    fn create_log_manager() {
        let mut file_mgr = FileManager::with_default_block_size(DB_DIR_TEST);
        let log_mgr = LogManager::new(&mut file_mgr, "log-file.dat");

        assert_eq!(0, log_mgr.cur_lsn);
        assert_eq!(0, log_mgr.last_saved_lsn);
    }
}
