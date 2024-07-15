use crate::storage::file_manager::{FileManager, DEFAULT_BLOCK_SIZE};

mod storage;
mod write_ahead_log;
mod utils;

const DB_DIR: &str = "/Users/mstepan/repo-rust/simpledb/db";

fn main() {
    let mut file_mgr = FileManager::new(DB_DIR, DEFAULT_BLOCK_SIZE);

    file_mgr.append("log.dat");

    println!("SimpleDB started");
}
