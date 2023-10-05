#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    buffer: Vec<u8>,
    forward_pos: usize,
    backward_pos: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            buffer: vec![0; block_size],
            forward_pos: 0,
            backward_pos: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let remained_bytes = self.backward_pos - self.forward_pos - 3;
        let entry_bytes = key.len() + value.len() + 4;
        if key.len() + value.len() + 4 > remained_bytes {
            return false;
        } else {
            self.append_entry_bytes(&(key.len() as u16).to_le_bytes());
            self.append_entry_bytes(key);
            self.append_entry_bytes(&(value.len() as u16).to_le_bytes());
            self.append_entry_bytes(value);

            self.add_offset(entry_bytes as u16);

            return true;
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        unimplemented!()
    }

    fn last_element_offset(&self) -> usize {
        return 0;
    }

    fn append_entry_bytes(&mut self, bytes: &[u8]) {
        self.buffer[self.forward_pos..self.forward_pos + bytes.len()].copy_from_slice(bytes);
        self.forward_pos += bytes.len();
    }

    fn add_offset(&mut self, entry_bytes: u16) {}
}
