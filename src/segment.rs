//! Segment V2 文件读写
//!
//! 文件格式：
//! ```text
//! ┌────────────────────────────────────┐
//! │  MAGIC  (8 bytes) "OLAPSEG\0"      │
//! │  Version(4 bytes) = 2              │
//! ├────────────────────────────────────┤
//! │  DATA REGION                       │
//! │    [Data Pages col 0]              │ ← LZ4 + 编码
//! │    [Data Pages col 1]              │
//! │    ...                             │
//! ├────────────────────────────────────┤
//! │  INDEX REGION                      │
//! │    [OrdinalIndex  col N]           │
//! │    [ZoneMapIndex  col N]           │
//! │    [BloomFilter   col N]           │
//! │    [ShortKeyIndex]                 │
//! ├────────────────────────────────────┤
//! │  FOOTER                            │
//! │    SegmentFooter (自定义二进制)      │
//! │    Footer CRC32  (4 bytes)         │
//! │    Footer length (4 bytes)         │
//! │    MAGIC         (8 bytes)         │
//! └────────────────────────────────────┘
//! ```

use std::io::Write;
use crate::common::{OlapError, Result};
use crate::column_writer::{ColumnWriter, ShortKeyIndexBuilder};
use crate::field_type::{ColumnMeta, Value};
use crate::index::{BloomFilter, OrdinalIndex, ZoneMapIndex};

const MAGIC: &[u8; 8] = b"OLAPSEG\0";
const VERSION: u32     = 2;

// ── Footer 结构 ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ColumnIndexMeta {
    pub ordinal_offset: u64,
    pub ordinal_size:   u64,
    pub zonemap_offset: u64,
    pub zonemap_size:   u64,
    pub bf_offset:      u64,
    pub bf_size:        u64,
}

#[derive(Debug)]
pub struct SegmentFooter {
    pub num_rows:         u32,
    pub num_columns:      u32,
    pub short_key_offset: u64,
    pub short_key_size:   u64,
    pub column_metas:     Vec<ColumnIndexMeta>,
}

impl SegmentFooter {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.num_rows.to_le_bytes());
        out.extend_from_slice(&self.num_columns.to_le_bytes());
        out.extend_from_slice(&self.short_key_offset.to_le_bytes());
        out.extend_from_slice(&self.short_key_size.to_le_bytes());
        for cm in &self.column_metas {
            out.extend_from_slice(&cm.ordinal_offset.to_le_bytes());
            out.extend_from_slice(&cm.ordinal_size.to_le_bytes());
            out.extend_from_slice(&cm.zonemap_offset.to_le_bytes());
            out.extend_from_slice(&cm.zonemap_size.to_le_bytes());
            out.extend_from_slice(&cm.bf_offset.to_le_bytes());
            out.extend_from_slice(&cm.bf_size.to_le_bytes());
        }
        out
    }

    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 24 { return None; }
        let num_rows    = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let num_columns = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let sk_offset   = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let sk_size     = u64::from_le_bytes(data[16..24].try_into().ok()?);
        let mut pos     = 24usize;
        let mut column_metas = Vec::new();
        for _ in 0..num_columns {
            if pos + 48 > data.len() { break; }
            let cm = ColumnIndexMeta {
                ordinal_offset: u64::from_le_bytes(data[pos..pos+8].try_into().ok()?),
                ordinal_size:   u64::from_le_bytes(data[pos+8..pos+16].try_into().ok()?),
                zonemap_offset: u64::from_le_bytes(data[pos+16..pos+24].try_into().ok()?),
                zonemap_size:   u64::from_le_bytes(data[pos+24..pos+32].try_into().ok()?),
                bf_offset:      u64::from_le_bytes(data[pos+32..pos+40].try_into().ok()?),
                bf_size:        u64::from_le_bytes(data[pos+40..pos+48].try_into().ok()?),
            };
            column_metas.push(cm);
            pos += 48;
        }
        Some(Self {
            num_rows, num_columns,
            short_key_offset: sk_offset, short_key_size: sk_size,
            column_metas,
        })
    }
}

// ── SegmentWriter ─────────────────────────────────────────────────────────────

pub struct SegmentWriter {
    schema:      Vec<ColumnMeta>,
    col_writers: Vec<ColumnWriter>,
    sk_builder:  ShortKeyIndexBuilder,
    num_rows:    u32,
    /// key 列在 schema 中的索引
    key_col_ids: Vec<usize>,
}

impl SegmentWriter {
    pub fn new(schema: Vec<ColumnMeta>) -> Self {
        let key_col_ids: Vec<usize> = (0..schema.len()).collect(); // 简化：前几列为 key
        let col_writers: Vec<ColumnWriter> = schema.iter()
            .map(|m| ColumnWriter::new(m.clone()))
            .collect();
        Self {
            schema,
            col_writers,
            sk_builder: ShortKeyIndexBuilder::new(),
            num_rows: 0,
            key_col_ids,
        }
    }

    /// 追加一行，`row` 的长度必须等于列数
    pub fn append_row(&mut self, row: Vec<Value>) -> Result<()> {
        if row.len() != self.col_writers.len() {
            return Err(OlapError::SchemaMismatch);
        }

        // ShortKey 每 1024 行记录一次前缀
        let key_vals: Vec<Value> = self.key_col_ids.iter()
            .filter_map(|&i| row.get(i).cloned())
            .collect();
        self.sk_builder.maybe_add(self.num_rows, &key_vals);

        for (i, col) in self.col_writers.iter_mut().enumerate() {
            col.add_value(row[i].clone())?;
        }
        self.num_rows += 1;
        Ok(())
    }

    /// 完成写入，将整个 Segment 序列化到字节流
    pub fn finalize<W: Write>(mut self, mut writer: W) -> Result<u64> {
        let mut pos: u64 = 0;

        // ── 文件头 ────────────────────────────────────────────────────────────
        writer.write_all(MAGIC).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        writer.write_all(&VERSION.to_le_bytes()).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        pos += 12;

        // ── DATA REGION ───────────────────────────────────────────────────────
        let num_cols = self.col_writers.len();
        let mut col_data:      Vec<Vec<u8>>      = Vec::with_capacity(num_cols);
        let mut col_offsets:   Vec<u64>          = Vec::with_capacity(num_cols);
        let mut ordinal_idxs:  Vec<OrdinalIndex> = Vec::new();
        let mut zonemap_idxs:  Vec<ZoneMapIndex> = Vec::new();
        let mut bloom_filters: Vec<BloomFilter>  = Vec::new();

        for cw in self.col_writers {
            let ordinal = cw.ordinal_index.clone();
            let zonemap = cw.zone_map.clone();
            let bf      = cw.bloom_filter.clone();
            ordinal_idxs.push(ordinal);
            zonemap_idxs.push(zonemap);
            bloom_filters.push(bf);

            let (data, _) = cw.finalize()?;
            col_offsets.push(pos);
            writer.write_all(&data).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
            pos += data.len() as u64;
            col_data.push(data);
        }

        // ── INDEX REGION ──────────────────────────────────────────────────────
        let mut col_index_metas: Vec<ColumnIndexMeta> = Vec::new();

        for i in 0..num_cols {
            let ord_bytes = ordinal_idxs[i].serialize();
            let zm_bytes  = zonemap_idxs[i].serialize();
            let bf_bytes  = bloom_filters[i].serialize();

            let cm = ColumnIndexMeta {
                ordinal_offset: pos,
                ordinal_size:   ord_bytes.len() as u64,
                zonemap_offset: pos + ord_bytes.len() as u64,
                zonemap_size:   zm_bytes.len() as u64,
                bf_offset:      pos + ord_bytes.len() as u64 + zm_bytes.len() as u64,
                bf_size:        bf_bytes.len() as u64,
            };

            writer.write_all(&ord_bytes).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
            writer.write_all(&zm_bytes).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
            writer.write_all(&bf_bytes).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
            pos += (ord_bytes.len() + zm_bytes.len() + bf_bytes.len()) as u64;

            col_index_metas.push(cm);
        }

        // ShortKey Index
        let sk_bytes  = self.sk_builder.index.serialize();
        let sk_offset = pos;
        let sk_size   = sk_bytes.len() as u64;
        writer.write_all(&sk_bytes).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        pos += sk_size;

        // ── FOOTER ────────────────────────────────────────────────────────────
        let footer = SegmentFooter {
            num_rows:         self.num_rows,
            num_columns:      num_cols as u32,
            short_key_offset: sk_offset,
            short_key_size:   sk_size,
            column_metas:     col_index_metas,
        };

        let footer_bytes = footer.serialize();
        let footer_crc   = crc32fast::hash(&footer_bytes);
        let footer_len   = footer_bytes.len() as u32;

        writer.write_all(&footer_bytes).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        writer.write_all(&footer_crc.to_le_bytes()).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        writer.write_all(&footer_len.to_le_bytes()).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        writer.write_all(MAGIC).map_err(|e| OlapError::SegmentIo(e.to_string()))?;
        pos += footer_bytes.len() as u64 + 16;

        Ok(pos)
    }

    pub fn num_rows(&self) -> u32 { self.num_rows }
    pub fn schema(&self) -> &[ColumnMeta] { &self.schema }
}

// ── SegmentReader ─────────────────────────────────────────────────────────────

pub struct SegmentReader {
    data:   Vec<u8>,
    footer: SegmentFooter,
    schema: Vec<ColumnMeta>,
}

impl SegmentReader {
    /// 从内存字节解析 Segment
    pub fn open(data: Vec<u8>, schema: Vec<ColumnMeta>) -> Result<Self> {
        let n = data.len();
        if n < 20 || &data[n-8..] != MAGIC {
            return Err(OlapError::SegmentIo("invalid segment magic".into()));
        }
        let footer_len   = u32::from_le_bytes(data[n-12..n-8].try_into().unwrap()) as usize;
        let footer_crc   = u32::from_le_bytes(data[n-16..n-12].try_into().unwrap());
        let footer_start = n - 16 - footer_len;
        let footer_bytes = &data[footer_start..footer_start + footer_len];

        if crc32fast::hash(footer_bytes) != footer_crc {
            return Err(OlapError::ChecksumMismatch);
        }

        let footer = SegmentFooter::deserialize(footer_bytes)
            .ok_or_else(|| OlapError::SegmentIo("cannot parse footer".into()))?;

        Ok(Self { data, footer, schema })
    }

    pub fn num_rows(&self) -> u32 { self.footer.num_rows }

    /// 读取指定列的所有页值（简化实现：返回所有值）
    pub fn read_column(&self, col_idx: usize) -> Result<Vec<Value>> {
        use crate::page::PageDecoder;

        let cm   = self.footer.column_metas.get(col_idx)
            .ok_or_else(|| OlapError::SegmentIo(format!("col {col_idx} not found")))?;
        let meta = self.schema.get(col_idx)
            .ok_or_else(|| OlapError::SegmentIo("schema mismatch".into()))?;

        // OrdinalIndex 告诉我们每页的偏移
        let ord_data  = &self.data[cm.ordinal_offset as usize
            ..(cm.ordinal_offset + cm.ordinal_size) as usize];
        let ord_index = OrdinalIndex::deserialize(ord_data);

        let mut all_values = Vec::new();
        let page_count = ord_index.page_count();

        for page_idx in 0..page_count {
            let page_off = ord_index.find_page_offset(page_idx as u32 * 1024)
                .unwrap_or(0) as usize;
            let next_off = if page_idx + 1 < page_count {
                ord_index.find_page_offset((page_idx as u32 + 1) * 1024)
                    .unwrap_or_else(|| cm.ordinal_offset) as usize
            } else {
                cm.ordinal_offset as usize
            };

            if page_off >= next_off || next_off > self.data.len() {
                continue;
            }
            let page_data = &self.data[page_off..next_off];
            match PageDecoder::decode(page_data, meta.encoding, meta.compression) {
                Ok(decoded) => all_values.extend(decoded.values),
                Err(_)      => {} // 容错：跳过损坏页
            }
        }

        Ok(all_values)
    }
}
