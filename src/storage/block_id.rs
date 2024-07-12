#![allow(dead_code)]

use std::hash::{Hash, Hasher};

///
/// BlockID.
///
/// Represents logical block ID inside file system.
///
#[derive(PartialEq, Debug)]
pub struct BlockId {
    pub file_name: String,
    pub block_no: u64,
}

impl BlockId {
    pub fn new(file_name: String, block_no: u64) -> Self {
        Self {
            file_name,
            block_no,
        }
    }
}

impl Hash for BlockId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.block_no.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_id_equals() {
        assert_eq!(
            BlockId::new("file111".to_string(), 111),
            BlockId::new("file111".to_string(), 111)
        );
        assert_eq!(
            BlockId::new("file222".to_string(), 222),
            BlockId::new("file222".to_string(), 222)
        );
    }

    #[test]
    fn block_id_not_equals() {
        assert_ne!(
            BlockId::new("file1".to_string(), 1),
            BlockId::new("file2".to_string(), 1)
        );
        assert_ne!(
            BlockId::new("file1".to_string(), 1),
            BlockId::new("file1".to_string(), 2)
        );
    }
}
