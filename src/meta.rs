//! Tablet 持久化元数据

use std::collections::HashMap;
use crate::common::{
    AggregateType, ColumnType, OlapError, KeysType,
    PartitionId, Result, RowsetId, SchemaHash, TabletId, Version,
};

// ── 列定义 ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub column_id:      u32,
    pub name:           String,
    pub column_type:    ColumnType,
    pub is_key:         bool,
    pub is_nullable:    bool,
    pub aggregate_type: AggregateType,
    /// VARCHAR 最大字节长度
    pub length:         u32,
}

impl ColumnSchema {
    /// 构建 key 列
    pub fn key(id: u32, name: &str, column_type: ColumnType) -> Self {
        Self {
            column_id: id, name: name.into(),
            column_type, is_key: true, is_nullable: false,
            aggregate_type: AggregateType::None, length: 0,
        }
    }
    /// 构建 value 列
    pub fn value(id: u32, name: &str, column_type: ColumnType, agg: AggregateType) -> Self {
        Self {
            column_id: id, name: name.into(),
            column_type, is_key: false, is_nullable: true,
            aggregate_type: agg, length: 0,
        }
    }
    /// 构建 VARCHAR 列
    pub fn varchar(id: u32, name: &str, max_len: u32, is_key: bool) -> Self {
        Self {
            column_id: id, name: name.into(),
            column_type: ColumnType::Varchar,
            is_key, is_nullable: !is_key,
            aggregate_type: AggregateType::None, length: max_len,
        }
    }
}

// ── Tablet Schema ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TabletSchema {
    pub schema_version:         u32,
    pub keys_type:              KeysType,
    pub columns:                Vec<ColumnSchema>,
    pub schema_hash:            SchemaHash,
    pub num_rows_per_row_block: u32,
}

impl TabletSchema {
    pub fn new(keys_type: KeysType, columns: Vec<ColumnSchema>) -> Self {
        // 简化 schema_hash：各列 id 的 xor
        let hash = columns.iter().fold(0u32, |h, c| h ^ (c.column_id * 2654435761));
        Self {
            schema_version: 1, keys_type, columns,
            schema_hash: hash, num_rows_per_row_block: 1024,
        }
    }

    pub fn key_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.columns.iter().filter(|c| c.is_key)
    }
    pub fn value_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.columns.iter().filter(|c| !c.is_key)
    }
    pub fn num_columns(&self) -> usize { self.columns.len() }
}

// ── Rowset 状态 ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowsetState { Prepared, Committed, Visible, Stale }

// ── Rowset 元数据 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RowsetMeta {
    pub rowset_id:      RowsetId,
    pub tablet_id:      TabletId,
    pub partition_id:   PartitionId,
    pub version:        Version,
    pub num_rows:       u64,
    pub data_disk_size: u64,
    pub num_segments:   u32,
    pub state:          RowsetState,
    /// 对应的 Segment 文件相对路径列表
    pub segment_paths:  Vec<String>,
}

impl RowsetMeta {
    pub fn new(
        rowset_id:      RowsetId,
        tablet_id:      TabletId,
        partition_id:   PartitionId,
        version:        Version,
        num_rows:       u64,
        data_disk_size: u64,
    ) -> Self {
        let num_segments = ((num_rows / 1_000_000) + 1) as u32;
        let segment_paths = (0..num_segments)
            .map(|i| format!("{}_{}_{}.seg", tablet_id, rowset_id, i))
            .collect();
        Self {
            rowset_id, tablet_id, partition_id, version,
            num_rows, data_disk_size, num_segments,
            state: RowsetState::Prepared, segment_paths,
        }
    }

    pub fn is_visible(&self) -> bool { self.state == RowsetState::Visible }
    pub fn mark_stale(&mut self) { self.state = RowsetState::Stale; }
}

// ── Tablet 元数据 ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct TabletMeta {
    pub tablet_id:    TabletId,
    pub partition_id: PartitionId,
    pub schema_hash:  SchemaHash,
    pub schema:       TabletSchema,
    /// rowset_id → RowsetMeta
    pub rowsets:      HashMap<RowsetId, RowsetMeta>,
    /// cumulative compaction 分界点
    pub cumulative_layer_point: i64,
    /// 最新可见版本
    pub max_version:  i64,
}

impl TabletMeta {
    pub fn new(tablet_id: TabletId, partition_id: PartitionId, schema: TabletSchema) -> Self {
        let schema_hash = schema.schema_hash;
        Self {
            tablet_id, partition_id, schema_hash, schema,
            rowsets: HashMap::new(),
            cumulative_layer_point: -1,
            max_version: -1,
        }
    }
}
