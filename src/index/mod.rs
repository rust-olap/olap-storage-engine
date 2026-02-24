//! 索引结构（对应 OLAP Segment V2 index/）
//!
//! 四种索引：
//! - **OrdinalIndex**  — 稀疏行号→页偏移，O(log n) 按 row_id 定位页
//! - **ZoneMapIndex**  — 每页 min/max，range 查询时跳过无关页
//! - **ShortKeyIndex** — 每 1024 行记录一次 key 前缀，有序扫描加速
//! - **BloomFilter**   — 双哈希位数组，等值查询快速过滤，FPP ≈ 5%

use std::collections::HashMap;

// ── Ordinal Index ─────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct OrdinalIndex {
    /// (first_row_id, page_file_offset) 有序列表
    entries: Vec<(u32, u64)>,
}

impl OrdinalIndex {
    pub fn add(&mut self, first_row_id: u32, page_offset: u64) {
        self.entries.push((first_row_id, page_offset));
    }

    /// 找包含 row_id 的页面偏移（二分查找）
    pub fn find_page_offset(&self, row_id: u32) -> Option<u64> {
        if self.entries.is_empty() { return None; }
        let pos = self.entries.partition_point(|(rid, _)| *rid <= row_id);
        let idx = if pos == 0 { 0 } else { pos - 1 };
        self.entries.get(idx).map(|(_, off)| *off)
    }

    pub fn page_count(&self) -> usize { self.entries.len() }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (rid, off) in &self.entries {
            out.extend_from_slice(&rid.to_le_bytes());
            out.extend_from_slice(&off.to_le_bytes());
        }
        out
    }

    pub fn deserialize(data: &[u8]) -> Self {
        if data.len() < 4 { return Self::default(); }
        let n = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(n);
        for i in 0..n {
            let b = 4 + i * 12;
            if b + 12 > data.len() { break; }
            let rid = u32::from_le_bytes(data[b..b+4].try_into().unwrap());
            let off = u64::from_le_bytes(data[b+4..b+12].try_into().unwrap());
            entries.push((rid, off));
        }
        Self { entries }
    }
}

// ── Zone Map Index ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ZoneMapEntry {
    pub page_idx: u32,
    pub min:      Vec<u8>,
    pub max:      Vec<u8>,
    pub has_null: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ZoneMapIndex {
    entries: Vec<ZoneMapEntry>,
}

impl ZoneMapIndex {
    pub fn add_page(
        &mut self,
        page_idx: u32,
        min:      Vec<u8>,
        max:      Vec<u8>,
        has_null: bool,
    ) {
        self.entries.push(ZoneMapEntry { page_idx, min, max, has_null });
    }

    /// 返回与 [probe_min, probe_max] 有重叠的页面索引列表
    pub fn filter(&self, probe_min: &[u8], probe_max: &[u8]) -> Vec<u32> {
        self.entries.iter()
            .filter(|e| {
                e.min.as_slice() <= probe_max && e.max.as_slice() >= probe_min
            })
            .map(|e| e.page_idx)
            .collect()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.page_idx.to_le_bytes());
            out.push(e.has_null as u8);
            out.extend_from_slice(&(e.min.len() as u32).to_le_bytes());
            out.extend_from_slice(&e.min);
            out.extend_from_slice(&(e.max.len() as u32).to_le_bytes());
            out.extend_from_slice(&e.max);
        }
        out
    }
}

// ── Short Key Index ───────────────────────────────────────────────────────────

pub const SHORT_KEY_INTERVAL: u32 = 1024;

#[derive(Debug, Default)]
pub struct ShortKeyIndex {
    /// (row_id, key_prefix_bytes)
    entries: Vec<(u32, Vec<u8>)>,
}

impl ShortKeyIndex {
    /// 每隔 SHORT_KEY_INTERVAL 行插入一条记录
    pub fn maybe_add(&mut self, row_id: u32, key_prefix: Vec<u8>) {
        if row_id % SHORT_KEY_INTERVAL == 0 {
            self.entries.push((row_id, key_prefix));
        }
    }

    /// 返回 key_prefix >= probe 的最小 row_id（下界查找）
    pub fn lower_bound(&self, probe: &[u8]) -> u32 {
        self.entries.iter()
            .take_while(|(_, pfx)| pfx.as_slice() < probe)
            .last()
            .map(|(rid, _)| *rid)
            .unwrap_or(0)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (rid, pfx) in &self.entries {
            out.extend_from_slice(&rid.to_le_bytes());
            out.extend_from_slice(&(pfx.len() as u32).to_le_bytes());
            out.extend_from_slice(pfx);
        }
        out
    }
}

// ── Bloom Filter ──────────────────────────────────────────────────────────────

/// 双哈希 Bloom Filter（FNV-1a），FPP ≈ 5%，7 个哈希函数
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits:     Vec<u8>,
    num_bits: usize,
}

impl BloomFilter {
    /// 根据期望基数创建（num_bits ≈ ndv × 9.6）
    pub fn new(expected_ndv: usize) -> Self {
        let num_bits  = (expected_ndv * 10).max(64);
        let num_bytes = (num_bits + 7) / 8;
        Self { bits: vec![0u8; num_bytes], num_bits }
    }

    fn probe_bits(value: &[u8]) -> impl Iterator<Item = usize> + '_ {
        let mut h1: u64 = 0xcbf29ce484222325;
        let mut h2: u64 = 0x517cc1b727220a95;
        for &b in value {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
            h2 ^= b as u64;
            h2 = h2.wrapping_mul(0x00000100000001b3);
        }
        let nb = h1.wrapping_add((h2 >> 32) ^ h2); // mix
        (0u64..7).map(move |i| {
            (nb.wrapping_add(i.wrapping_mul(h1)) % h1.max(1)) as usize
        })
    }

    pub fn add(&mut self, value: &[u8]) {
        let nb = self.num_bits;
        for bit in Self::probe_bits(value) {
            let bit = bit % nb;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    pub fn may_contain(&self, value: &[u8]) -> bool {
        for bit in Self::probe_bits(value) {
            let bit = bit % self.num_bits;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn deserialize(data: &[u8]) -> Self {
        if data.len() < 4 { return Self { bits: vec![], num_bits: 0 }; }
        let num_bits = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        Self { bits: data[4..].to_vec(), num_bits }
    }
}
