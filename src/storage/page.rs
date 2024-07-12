#![allow(dead_code)]

use std::str;

///
/// PAGE.
///
/// Represents in-memory buffer that can be written into file system.
///
#[derive(Debug)]
pub struct Page {
    pub data: Vec<u8>,
}

const INTEGER_SIZE_IN_BYTES: usize = 4;

impl Page {
    pub fn new(size: u64) -> Self {
        Self {
            data: vec![0; size as usize],
        }
    }

    #[allow(dead_code)]
    pub fn from_raw_buf(data: Vec<u8>) -> Self {
        Self { data }
    }

    ///
    /// Store/Read string slice.
    ///
    pub fn put_string(&mut self, offset: usize, buf: &str) {
        self.put_bytes(offset, buf.as_bytes());
    }

    pub fn get_string(&mut self, offset: usize) -> &str {
        str::from_utf8(self.get_bytes(offset)).expect("Can't read &str from Page offset")
    }

    ///
    /// Store/Read slice of bytes
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
    /// Store/Load signed int.
    ///
    pub fn put_i32(&mut self, offset: usize, value: i32) {
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }
    pub fn get_i32(&self, offset: usize) -> i32 {
        i32::from_be_bytes(
            self.data[offset..offset + INTEGER_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    ///
    /// Store/Load unsigned int.
    ///
    pub fn put_u32(&mut self, offset: usize, value: u32) {
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }
    pub fn get_u32(&self, offset: usize) -> u32 {
        u32::from_be_bytes(
            self.data[offset..offset + INTEGER_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_store_load_string() {
        let mut page = Page::new(1024);
        page.put_string(17, "Hello, world!!!");
        assert_eq!("Hello, world!!!", page.get_string(17));
    }

    #[test]
    fn page_store_load_bytes() {
        let mut page = Page::new(4096);
        page.put_bytes(100, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], page.get_bytes(100));
    }

    #[test]
    fn page_store_load_positive_int() {
        let mut page = Page::new(4096);
        page.put_u32(10, 0x0D_CC_BB_AA);
        assert_eq!(0x0D_CC_BB_AA, page.get_u32(10));
    }

    #[test]
    fn page_store_load_negative_int() {
        let mut page = Page::new(4096);
        page.put_i32(10, -123_456_789);
        assert_eq!(-123_456_789, page.get_i32(10));
    }
}