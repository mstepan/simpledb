use std::fs::{metadata, remove_dir_all};

use crate::storage::file_manager::FileManager;
use crate::write_ahead_log::log_manager::LogManager;

mod storage;
mod utils;
mod write_ahead_log;

const DB_DIR: &str = "/Users/mstepan/repo-rust/simpledb/db";

fn main() {
    if metadata(DB_DIR).is_ok() {
        remove_dir_all(DB_DIR).expect("Can't delete 'DB_DIR' directory");
    }

    let mut file_mgr = FileManager::new(DB_DIR, 20);

    let mut log_manager = LogManager::new(&mut file_mgr, "log-file.data");

    let mut lsn = 0;

    for i in 0..10 {
        let msg = format!("message-{}", i);
        lsn = log_manager.append(msg.as_bytes());
    }
    log_manager.flush(lsn);

    println!("SimpleDB started");
}
