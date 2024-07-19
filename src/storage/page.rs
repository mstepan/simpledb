#![allow(dead_code)]

use crate::utils::primitive_types::{
    INTEGER_SIZE_IN_BYTES, LONG_SIZE_IN_BYTES, SHORT_SIZE_IN_BYTES,
};
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

#[derive(Debug, Clone)]
pub struct PageOverflow {
    page_size: usize,
    offset: usize,
    data_size: usize,
}

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

    pub fn block_size(&self) -> usize {
        return self.data.len();
    }

    ///
    /// Store/Read string slice.
    ///
    pub fn put_string(&mut self, offset: usize, buf: &str) -> Result<(), PageOverflow> {
        self.put_bytes(offset, buf.as_bytes())?;
        return Ok(());
    }

    pub fn get_string(&mut self, offset: usize) -> &str {
        str::from_utf8(self.get_bytes(offset)).expect("Can't read &str from Page offset")
    }

    ///
    /// Store/Read slice of bytes
    ///
    pub fn put_bytes(&mut self, offset: usize, buf: &[u8]) -> Result<(), PageOverflow> {
        self.check_boundary(offset, buf.len() + INTEGER_SIZE_IN_BYTES)?;

        self.put_u32(offset, buf.len() as u32);

        let new_offset = offset + INTEGER_SIZE_IN_BYTES;

        self.data[new_offset..new_offset + buf.len()].copy_from_slice(&buf);

        return Ok(());
    }

    pub fn get_bytes(&mut self, offset: usize) -> &[u8] {
        let buf_size_in_bytes = self.get_u32(offset);
        let new_offset = offset + INTEGER_SIZE_IN_BYTES;

        &self.data[new_offset..new_offset + buf_size_in_bytes as usize]
    }

    ///
    /// Store/Load boolean value.
    /// Boolean value will be stored us u8, with 1 indicating TRUE and 0 indicating FALSE.
    ///
    pub fn put_bool(&mut self, offset: usize, value: bool) -> Result<(), PageOverflow> {
        self.check_boundary(offset, 1)?;

        let value_as_u8 = if value { 1 } else { 0 };

        self.data[offset] = value_as_u8;
        return Ok(());
    }
    pub fn get_bool(&self, offset: usize) -> bool {
        let bool_as_u8 = self.data[offset];

        return if bool_as_u8 == 0 { false } else { true };
    }

    ///
    /// Store/Load unsigned short.
    ///
    pub fn put_u16(&mut self, offset: usize, value: u16) -> Result<(), PageOverflow> {
        self.check_boundary(offset, SHORT_SIZE_IN_BYTES)?;
        self.data[offset..offset + SHORT_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
        return Ok(());
    }
    pub fn get_u16(&self, offset: usize) -> u16 {
        u16::from_be_bytes(
            self.data[offset..offset + SHORT_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    ///
    /// Store/Load signed int.
    ///
    pub fn put_i32(&mut self, offset: usize, value: i32) -> Result<(), PageOverflow> {
        self.check_boundary(offset, INTEGER_SIZE_IN_BYTES)?;
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
        return Ok(());
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
        //TODO: handle page overflow here
        self.data[offset..offset + INTEGER_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }
    pub fn get_u32(&self, offset: usize) -> u32 {
        u32::from_be_bytes(
            self.data[offset..offset + INTEGER_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    ///
    /// Store/load unsigned long.
    ///
    pub fn put_u64(&mut self, offset: usize, value: u64) {
        //TODO: handle page overflow here
        self.data[offset..offset + LONG_SIZE_IN_BYTES].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_u64(&self, offset: usize) -> u64 {
        u64::from_be_bytes(
            self.data[offset..offset + LONG_SIZE_IN_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    fn check_boundary(&self, offset: usize, elements_size: usize) -> Result<(), PageOverflow> {
        if offset + elements_size > self.data.len() {
            return Err(PageOverflow {
                page_size: self.block_size(),
                offset,
                data_size: elements_size,
            });
        }

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_string() {
        let mut page = Page::new(1024);
        page.put_string(117, "Hello, world!!!").unwrap();
        assert_eq!("Hello, world!!!", page.get_string(117));

        page.put_string(0, "message-123").unwrap();
        assert_eq!("message-123", page.get_string(0));
        assert_eq!("Hello, world!!!", page.get_string(117));
    }

    #[test]
    fn put_get_string_with_page_overflow() {
        let mut page = Page::new(128);
        let res = page.put_string(200, "message-with-overflow");
        assert!(matches!(res, Err(_)));
    }

    #[test]
    fn put_get_bytes() {
        let mut page = Page::new(4096);
        page.put_bytes(100, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            .unwrap();
        assert_eq!(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], page.get_bytes(100));
    }

    #[test]
    fn put_get_positive_i32() {
        let mut page = Page::new(4096);
        page.put_u32(10, 0x0D_CC_BB_AA);
        assert_eq!(0x0D_CC_BB_AA, page.get_u32(10));
    }

    #[test]
    fn put_i32_into_last_position() {
        let mut page = Page::new(128);
        let pos = page.block_size() - INTEGER_SIZE_IN_BYTES;
        page.put_i32(pos, -123456789)
            .expect("PageOverflow occurred");
        assert_eq!(-123456789, page.get_i32(pos));
    }

    #[test]
    fn put_get_negative_i32() {
        let mut page = Page::new(4096);
        page.put_i32(10, -123_456_789).unwrap();
        assert_eq!(-123_456_789, page.get_i32(10));
    }

    #[test]
    fn put_get_boolean() {
        let mut page = Page::new(1024);

        page.put_bool(0, true).unwrap();
        page.put_bool(1, true).unwrap();
        page.put_bool(2, false).unwrap();
        page.put_bool(3, false).unwrap();
        page.put_bool(4, true).unwrap();

        assert_eq!(true, page.get_bool(0));
        assert_eq!(true, page.get_bool(1));
        assert_eq!(false, page.get_bool(2));
        assert_eq!(false, page.get_bool(3));
        assert_eq!(true, page.get_bool(4));
    }
}
