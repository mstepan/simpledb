use std::hash::{Hash, Hasher};

///
/// FileMgr.
///
/// Represents the main interface to store and load data from file system into Page and back.
///
#[allow(dead_code)]
struct FileManager {}

impl FileManager {}

///
/// PAGE.
///
/// Represents in-memory buffer that can be written into file system.
///
#[derive(Debug)]
struct Page {
    data: Vec<u8>,
}

const INTEGER_SIZE_IN_BYTES: usize = 4;

impl Page {
    pub fn new(size: usize) -> Self {
        Self {
            data: vec![0; size],
        }
    }

    #[allow(dead_code)]
    pub fn wrap(data: Vec<u8>) -> Self {
        Self { data }
    }

    ///
    /// Store slice of bytes
    ///
    pub fn put_bytes(&mut self, offset: usize, buf: &[u8]) {
        self.put_u32(offset, buf.len() as u32);

        let new_offset = offset + INTEGER_SIZE_IN_BYTES;

        self.data[new_offset..new_offset + buf.len()].copy_from_slice(&buf);
    }

    pub fn get_bytes(&mut self, offset: usize) -> &[u8] {
        let buf_size_in_bytes = self.get_u32(offset);
        let new_offset = offset + INTEGER_SIZE_IN_BYTES;

        &self.data[new_offset..new_offset + buf_size_in_bytes as usize]
    }

    ///
    /// Store signed/unsigned int value in a BIG endian order at specified 'offset'
    ///
    pub fn put_i32(&mut self, offset: usize, value: i32) {
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }

    pub fn put_u32(&mut self, offset: usize, value: u32) {
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }

    ///
    /// Read signed/unsigned int value in BIG endian order from specified 'offset'
    ///
    pub fn get_i32(&self, offset: usize) -> i32 {
        i32::from_be_bytes(
            self.data[offset..offset + INTEGER_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    pub fn get_u32(&self, offset: usize) -> u32 {
        u32::from_be_bytes(
            self.data[offset..offset + INTEGER_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }
}

///
/// BlockID.
///
/// Represents logical block ID inside file system.
///
#[derive(PartialEq, Debug)]
struct BlockId {
    file_name: String,
    block_no: u64,
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
    fn page_read_write_bytes() {
        let mut page = Page::new(4096);
        page.put_bytes(100, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], page.get_bytes(100));
    }

    #[test]
    fn page_read_write_positive_int() {
        let mut page = Page::new(4096);
        page.put_u32(10, 0x0D_CC_BB_AA);
        assert_eq!(0x0D_CC_BB_AA, page.get_u32(10));
    }

    #[test]
    fn page_read_write_negative_int() {
        let mut page = Page::new(4096);
        page.put_i32(10, -123_456_789);
        assert_eq!(-123_456_789, page.get_i32(10));
    }

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
