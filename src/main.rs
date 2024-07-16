use crate::storage::file_manager::FileManager;
use crate::write_ahead_log::log_manager::LogManager;
use std::fs::{metadata, remove_dir_all};

mod storage;
mod utils;
mod write_ahead_log;

const DB_DIR: &str = "/Users/mstepan/repo-rust/simpledb/db";

fn main() {
    if metadata(DB_DIR).is_ok() {
        remove_dir_all(DB_DIR).expect("Can't delete 'DB_DIR' directory");
    }

    let mut file_mgr = FileManager::with_default_block_size(DB_DIR);
    // file_mgr.append("log.dat");

    let mut log_manager = LogManager::new(&mut file_mgr, "log-file.data");

    log_manager.append("hello, world!!!".as_bytes());

    println!("SimpleDB started");
}
