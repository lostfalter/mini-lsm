// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    buffer: Vec<u8>,
    element_count: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            buffer: Vec::with_capacity(block_size),
            element_count: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_bytes = key.len() + value.len() + 4;
        if !self.is_empty() && entry_bytes + self.current_ocuppied_bytes() > self.buffer.capacity()
        {
            false
        } else {
            self.buffer.put((key.len() as u16).to_le_bytes().as_slice());
            self.buffer.put(key);
            self.buffer
                .put((value.len() as u16).to_le_bytes().as_slice());
            self.buffer.put(value);

            self.element_count = self.element_count + 1;

            true
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        return self.element_count == 0;
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut block = Block {
            data: self.buffer,
            offsets: Vec::with_capacity(self.element_count),
        };

        Self::build_offsets(&mut block.offsets, &block.data);

        return block;
    }

    fn current_ocuppied_bytes(&self) -> usize {
        return self.buffer.len() + (self.element_count + 1) * 2;
    }

    fn build_offsets(offsets: &mut Vec<u16>, buffer: &Vec<u8>) {
        // scan the offset
        let mut last_offset = 0;
        let mut buf_pos = 0;
        while buf_pos < buffer.len() {
            let key_len = u16::from_le_bytes([buffer[buf_pos], buffer[buf_pos + 1]]);
            buf_pos += key_len as usize + 2;
            let value_len = u16::from_le_bytes([buffer[buf_pos], buffer[buf_pos + 1]]);
            buf_pos += value_len as usize + 2;

            offsets.push(last_offset);
            last_offset = last_offset + key_len + value_len + 4;
        }
    }
}
