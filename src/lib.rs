//! # olap-engine
//!
//! OLAP 存储引擎的 Rust 完整实现，合并自：
//! - **olap-storage-engine**：Table / Partition / Tablet 元数据管理层
//! - **olap-segment-engine**：Segment V2 列存储文件 I/O 层
//!
//! ## 整体架构
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    StorageEngine                         │
//! │   ┌─────────────────┐   ┌────────────────────────────┐  │
//! │   │  TabletManager  │   │     CatalogManager         │  │
//! │   │  (64-shard map) │   │  Database → OlapTable      │  │
//! │   └───────┬─────────┘   └──────────────┬─────────────┘  │
//! │           │                            │                 │
//! │        Tablet                      OlapTable             │
//! │     ┌────┴──────┐              ┌───────┴────────┐        │
//! │  TabletMeta  VersionGraph   TabletSchema  PartitionInfo  │
//! │     │                                                     │
//! │   Rowset  ←── publish_rowset() 将 RowsetMeta 注册进来    │
//! │     │                                                     │
//! │   Segment (列存文件，由 segment 层实际读写)               │
//! │   ┌────────────────────────────────────────────────┐     │
//! │   │  ColumnWriter × N                              │     │
//! │   │   ├─ encoding  (Plain/RLE/Delta/Dict)          │     │
//! │   │   ├─ compression (LZ4/None)                    │     │
//! │   │   ├─ OrdinalIndex  (行号→页偏移)                │     │
//! │   │   ├─ ZoneMapIndex  (min/max 剪枝)              │     │
//! │   │   └─ BloomFilter   (等值加速)                  │     │
//! │   │  ShortKeyIndex (段级稀疏前缀索引)               │     │
//! │   └────────────────────────────────────────────────┘     │
//! └─────────────────────────────────────────────────────────┘
//! ```

// ── Storage 层（来自 olap-storage-engine）─────────────────────────────────────
pub mod common;
pub mod meta;
pub mod partition;
pub mod tablet;
pub mod table;
pub mod storage;

// ── Segment 层（来自 olap-segment-engine）────────────────────────────────────
pub mod field_type;
pub mod encoding;
pub mod compression;
pub mod page;
pub mod index;
pub mod column_writer;
pub mod segment;
