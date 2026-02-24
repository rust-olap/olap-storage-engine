//! 压缩/解压（LZ4 / None）

use crate::common::{OlapError, Result};
use crate::field_type::CompressionType;

pub fn compress(data: &[u8], codec: CompressionType) -> Result<Vec<u8>> {
    match codec {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4  =>
            lz4::block::compress(data, None, false)
                .map_err(|e| OlapError::Compression(e.to_string())),
    }
}

pub fn decompress(
    data:             &[u8],
    codec:            CompressionType,
    uncompressed_len: usize,
) -> Result<Vec<u8>> {
    match codec {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4  =>
            lz4::block::decompress(data, Some(uncompressed_len as i32))
                .map_err(|e| OlapError::Compression(e.to_string())),
    }
}
