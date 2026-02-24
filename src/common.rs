//! 全局基础类型与错误定义

use thiserror::Error;

// ── ID 类型别名 ───────────────────────────────────────────────────────────────

pub type TabletId    = u64;
pub type PartitionId = u64;
pub type SchemaHash  = u32;
pub type DbId        = u64;
pub type TableId     = u64;
pub type RowsetId    = u64;

// ── Version ───────────────────────────────────────────────────────────────────

/// 一个 Rowset 覆盖的 [start, end] 闭区间版本范围
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    pub start: i64,
    pub end:   i64,
}

impl Version {
    pub fn new(start: i64, end: i64) -> Self { Self { start, end } }
    pub fn point(v: i64) -> Self { Self::new(v, v) }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{},{}]", self.start, self.end)
    }
}

// ── 枚举 ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysType {
    /// 聚合模型 — value 列有 SUM/MAX/MIN 等聚合函数
    Aggregate,
    /// 唯一键模型 — 相同 key 保留最新版本
    Unique,
    /// 明细模型 — 保留所有行
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int8, Int16, Int32, Int64,
    Float32, Float64,
    Varchar,
    Date,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    None, Sum, Max, Min, Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMedium { Hdd, Ssd }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionType { Base, Cumulative }

// ── 错误 ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum OlapError {
    #[error("tablet not found: tablet_id={0}")]
    TabletNotFound(TabletId),
    #[error("table not found: db_id={0} table_id={1}")]
    TableNotFound(DbId, TableId),
    #[error("database not found: db_id={0}")]
    DatabaseNotFound(DbId),
    #[error("partition not found for key: {0}")]
    PartitionNotFound(String),
    #[error("version already exists: {0}")]
    VersionExists(Version),
    #[error("missing versions in range {0}")]
    MissingVersions(String),
    #[error("segment I/O error: {0}")]
    SegmentIo(String),
    #[error("encoding error: {0}")]
    Encoding(String),
    #[error("compression error: {0}")]
    Compression(String),
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("schema mismatch")]
    SchemaMismatch,
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, OlapError>;
