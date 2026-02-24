//! 列写入器（对应 OLAP Segment V2 column_writer）
//!
//! 每列独立维护：
//!   - Data Page 缓冲区
//!   - OrdinalIndex（行号→页偏移）
//!   - ZoneMapIndex（min/max 剪枝）
//!   - BloomFilter（等值过滤）

use crate::common::Result;
use crate::field_type::{ColumnMeta, Value};
use crate::index::{BloomFilter, OrdinalIndex, ShortKeyIndex, ZoneMapIndex};
use crate::page::PageBuilder;

pub struct ColumnWriter {
    pub meta:           ColumnMeta,
    // 已完成的页（序列化字节）
    pages:              Vec<Vec<u8>>,
    current:            PageBuilder,
    // 当前行计数（跨页累计）
    next_row_id:        u32,
    // 页级别 min/max（用于 ZoneMap）
    page_min:           Option<Vec<u8>>,
    page_max:           Option<Vec<u8>>,
    page_ordinal:       u32,
    // 写入指针（数据区偏移）
    data_offset:        u64,
    // 索引
    pub ordinal_index:  OrdinalIndex,
    pub zone_map:       ZoneMapIndex,
    pub bloom_filter:   BloomFilter,
}

impl ColumnWriter {
    pub fn new(meta: ColumnMeta) -> Self {
        let bf = BloomFilter::new(4096);
        let page = PageBuilder::new(0, meta.encoding, meta.compression);
        Self {
            meta, pages: Vec::new(),
            current: page, next_row_id: 0,
            page_min: None, page_max: None,
            page_ordinal: 0, data_offset: 0,
            ordinal_index: OrdinalIndex::default(),
            zone_map: ZoneMapIndex::default(),
            bloom_filter: bf,
        }
    }

    /// 追加一个值到本列
    pub fn add_value(&mut self, value: Value) -> Result<()> {
        // 1. BloomFilter
        let key = value.to_sort_key();
        self.bloom_filter.add(&key);

        // 2. ZoneMap：更新页内 min/max
        if self.page_min.as_deref().map(|m| key.as_slice() < m).unwrap_or(true) {
            self.page_min = Some(key.clone());
        }
        if self.page_max.as_deref().map(|m| key.as_slice() > m).unwrap_or(true) {
            self.page_max = Some(key);
        }

        self.current.add(value);
        self.next_row_id += 1;

        if self.current.is_full() {
            self.flush_page()?;
        }
        Ok(())
    }

    fn flush_page(&mut self) -> Result<()> {
        let first_rid = self.current.first_row_id;
        let bytes     = std::mem::replace(
            &mut self.current,
            PageBuilder::new(self.next_row_id, self.meta.encoding, self.meta.compression),
        ).build()?;

        let page_len  = bytes.len() as u64;

        self.ordinal_index.add(first_rid, self.data_offset);
        self.zone_map.add_page(
            self.page_ordinal,
            self.page_min.take().unwrap_or_default(),
            self.page_max.take().unwrap_or_default(),
            false,
        );

        self.pages.push(bytes);
        self.data_offset  += page_len;
        self.page_ordinal += 1;
        Ok(())
    }

    /// 完成写入，返回列的数据字节和总字节数
    pub fn finalize(mut self) -> Result<(Vec<u8>, u64)> {
        if !self.current.is_empty() {
            self.flush_page()?;
        }
        let data: Vec<u8> = self.pages.into_iter().flatten().collect();
        let size = data.len() as u64;
        Ok((data, size))
    }

    pub fn num_rows(&self) -> u32 { self.next_row_id }
}

// ── ShortKeyIndexBuilder（仅 key 列使用）────────────────────────────────────

pub struct ShortKeyIndexBuilder {
    pub index: ShortKeyIndex,
}

impl ShortKeyIndexBuilder {
    pub fn new() -> Self {
        Self { index: ShortKeyIndex::default() }
    }

    pub fn maybe_add(&mut self, row_id: u32, key_columns: &[Value]) {
        // 将所有 key 列拼接为前缀
        let prefix: Vec<u8> = key_columns.iter()
            .flat_map(|v| v.to_sort_key())
            .collect();
        self.index.maybe_add(row_id, prefix);
    }
}
