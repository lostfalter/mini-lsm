// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::BufMut;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut bi = Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        };

        if !bi.block.offsets.is_empty() {
            bi.seek_to_index(0)
        }

        bi
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut bi = Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        };

        bi.seek_to_key(key);

        bi
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_index(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to_index(low);
    }

    fn acquire_entry(block: &Block, index: usize) -> &[u8] {
        assert!(index < block.offsets.len());

        if index == block.offsets.len() - 1 {
            // last element
            &block.data[block.offsets[index] as usize..block.data.len()]
        } else {
            &block.data[block.offsets[index] as usize..block.offsets[index + 1] as usize]
        }
    }

    fn seek_to_index(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        let entry = Self::acquire_entry(&self.block, index);

        self.idx = index;

        let mut pos = 0;

        let key_len = u16::from_le_bytes([entry[pos], entry[pos + 1]]) as usize;
        pos = pos + 2;

        self.key.clear();
        self.key.put(&entry[pos..pos + key_len]);
        pos = pos + key_len;

        let value_len = u16::from_le_bytes([entry[pos], entry[pos + 1]]) as usize;
        pos = pos + 2;

        self.value.clear();
        self.value.put(&entry[pos..pos + value_len]);
    }
}
