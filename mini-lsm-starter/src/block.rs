// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + (self.offsets.len() * 2) + 2);
        buf.put(self.data.as_slice());
        for offset in self.offsets.iter() {
            buf.put_u16_le(*offset);
        }
        buf.put_u16_le(self.offsets.len() as u16);

        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let element_count = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);

        let entry_bytes_count = data.len() - (2 * element_count as usize) - 2;

        let mut block = Block {
            data: Vec::with_capacity(entry_bytes_count),
            offsets: Vec::with_capacity(element_count as usize * 2),
        };

        block.data.put(&data[0..entry_bytes_count]);
        for i in 0..element_count as usize {
            block.offsets.push(u16::from_le_bytes([
                data[entry_bytes_count + (i * 2)],
                data[entry_bytes_count + (i * 2) + 1],
            ]));
        }

        block
    }
}

#[cfg(test)]
mod tests;
