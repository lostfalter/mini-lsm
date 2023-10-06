#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;

use crate::block::BlockBuilder;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    block_builder: BlockBuilder,
    block_size: usize,
    buffer: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size,
            buffer: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if !self.block_builder.add(key, value) {
            let last_block_builder =
                std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));

            let block = last_block_builder.build();
            let block_bytes = block.encode();

            let mut bm = BlockMeta {
                offset: 0,
                first_key: Bytes::new(),
            };

            if !self.meta.is_empty() {
                bm.offset = self.meta.last().unwrap().offset + block_bytes.len();
            } else {
                bm.offset = 0;
            }

            let first_key_len = u16::from_le_bytes([block_bytes[0], block_bytes[1]]) as usize;
            bm.first_key = Bytes::copy_from_slice(&block_bytes[2..2 + first_key_len]);

            self.meta.push(bm);

            self.buffer.put(block_bytes);
        }

        assert!(self.block_builder.add(key, value));
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.buffer.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        unimplemented!()
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
