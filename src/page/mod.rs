//! Data Page 读写
//!
//! ```text
//! ┌──────────────────────────────────┐
//! │ value_count  (u32 LE)            │
//! │ first_row_id (u32 LE)            │
//! │ uncomp_size  (u32 LE)            │
//! │ has_nulls    (u8)                │
//! │ [null_bitmap (bit-packed)]       │  仅 nullable 列
//! │ data         (encoded+compressed)│
//! │ CRC32        (u32 LE)            │
//! └──────────────────────────────────┘
//! ```

use crate::common::{OlapError, Result};
use crate::encoding;
use crate::compression;
use crate::field_type::{CompressionType, EncodingType, Value};

/// 每页最多容纳的行数
pub const PAGE_MAX_ROWS: usize = 1024;

// ── PageBuilder ───────────────────────────────────────────────────────────────

pub struct PageBuilder {
    pub first_row_id: u32,
    encoding:         EncodingType,
    compression:      CompressionType,
    values:           Vec<Value>,
}

impl PageBuilder {
    pub fn new(
        first_row_id: u32,
        encoding:     EncodingType,
        compression:  CompressionType,
    ) -> Self {
        Self { first_row_id, encoding, compression, values: Vec::new() }
    }

    pub fn add(&mut self, v: Value) {
        self.values.push(v);
    }

    pub fn len(&self)      -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool  { self.values.is_empty() }
    pub fn is_full(&self)  -> bool  { self.values.len() >= PAGE_MAX_ROWS }

    /// 序列化为页字节（encode → compress → 加 header+CRC）
    pub fn build(self) -> Result<Vec<u8>> {
        let count       = self.values.len() as u32;
        let encoded     = encoding::encode(&self.values, self.encoding)?;
        let uncomp_size = encoded.len() as u32;
        let compressed  = compression::compress(&encoded, self.compression)?;

        let mut page = Vec::new();
        page.extend_from_slice(&count.to_le_bytes());
        page.extend_from_slice(&self.first_row_id.to_le_bytes());
        page.extend_from_slice(&uncomp_size.to_le_bytes());
        page.push(0u8); // has_nulls = false（简化：不支持 null bitmap）
        page.extend_from_slice(&compressed);

        let crc = crc32fast::hash(&page);
        page.extend_from_slice(&crc.to_le_bytes());
        Ok(page)
    }
}

// ── PageDecoder ───────────────────────────────────────────────────────────────

pub struct PageDecoder {
    pub value_count:  usize,
    pub first_row_id: u32,
    pub values:       Vec<Value>,
}

impl PageDecoder {
    pub fn decode(
        data:        &[u8],
        encoding:    EncodingType,
        compression: CompressionType,
    ) -> Result<Self> {
        if data.len() < 17 {
            return Err(OlapError::SegmentIo("page data too short".into()));
        }
        let value_count  = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let first_row_id = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let uncomp_size  = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        // byte[12] = has_nulls，byte[13..] = payload，last 4 = CRC
        let payload_end  = data.len() - 4;
        let payload      = &data[13..payload_end];

        // 校验 CRC
        let stored_crc = u32::from_le_bytes(data[payload_end..].try_into().unwrap());
        let actual_crc = crc32fast::hash(&data[..payload_end]);
        if stored_crc != actual_crc {
            return Err(OlapError::ChecksumMismatch);
        }

        let raw    = compression::decompress(payload, compression, uncomp_size)?;
        let values = encoding::decode(&raw, encoding, value_count)?;

        Ok(Self { value_count, first_row_id, values })
    }
}
