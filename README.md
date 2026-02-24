# olap-storage-engine

---

## 整体架构

```
StorageEngine（顶层协调器）
├── TabletManager  ——— 64-shard RwLock 分片注册表
│   └── Tablet ——————— Arc<RwLock<TabletInner>>
│       ├── TabletMeta ── schema + rowsets map
│       └── VersionGraph ─ DAG of [start,end] rowset edges
│
├── CatalogManager ——— RwLock<HashMap<DbId, Database>>
│   └── OlapTable
│       ├── TabletSchema    (列定义, keys_type)
│       └── PartitionInfo   (Range / List / Unpartitioned)
│           └── Partition
│               └── MaterializedIndex → [TabletId × num_buckets]
│
└── Segment I/O 层（原 olap-segment-engine）
    ┌────────────────────────────────────────────────────────┐
    │  SegmentWriter                                          │
    │  ├── ColumnWriter × N                                  │
    │  │    ├── PageBuilder (encode → compress → CRC)        │
    │  │    ├── OrdinalIndex (row_id → page_offset)          │
    │  │    ├── ZoneMapIndex (min/max per page)               │
    │  │    └── BloomFilter  (FNV1a double-hash, FPP≈5%)      │
    │  └── ShortKeyIndex (sparse prefix, every 1024 rows)    │
    │                                                         │
    │  SegmentReader                                          │
    │  └── read_column(col_idx) → Vec<Value>                 │
    └────────────────────────────────────────────────────────┘
```

### OLAP 数据模型

```
Table
 └── Partition   (RANGE / LIST / UNPARTITIONED)
      └── Bucket  (HASH bucket_columns / RANDOM)
           └── Tablet   ← 一个桶的一个副本
                └── Rowset  (不可变 segment 捆绑, 覆盖 [start,end] 版本)
                     └── Segment  (列存文件, Segment V2 格式)
                          └── Column (encoding × compression)
```

---

## 模块对应关系

### Storage 层（来自 olap-storage-engine）

| Rust 模块 | 功能 |
|-----------|------|
| `common`    | 基础类型、`OlapError` |
| `meta`      | `TabletMeta`、`RowsetMeta` |
| `tablet`    | `Tablet`、`VersionGraph`、`TabletManager` |
| `partition` | Range/List 分区策略 |
| `table`     | `CatalogManager`、`OlapTable` |
| `storage`   | `StorageEngine` 顶层协调 |

### Segment 层（来自 olap-segment-engine）

| Rust 模块 | 功能 |
|-----------|------|
| `field_type`    | `FieldType`、`EncodingType`、`Value` |
| `encoding`      | Plain/RLE/Delta/Dict |
| `compression`   | LZ4/None |
| `page`          | Data Page 读写 + CRC |
| `index`         | Ordinal/ZoneMap/BloomFilter/ShortKey |
| `column_writer` | 列写入主逻辑 |
| `segment`       | Segment V2 文件读写 |

---

## Segment V2 文件格式

```
┌────────────────────────────────────────┐
│  MAGIC    (8 bytes)  "OLAPSEG\0"       │
│  Version  (4 bytes)  = 2               │
├────────────────────────────────────────┤
│  DATA REGION（按列分区存储）             │
│    [Data Pages for col 0]              │  ← 1024行/页, LZ4压缩
│    [Data Pages for col 1]              │
│    ...                                 │
├────────────────────────────────────────┤
│  INDEX REGION（按列粒度加载）            │
│    [OrdinalIndex  for col N]           │  ← 行号→页指针
│    [ZoneMapIndex  for col N]           │  ← min/max剪枝
│    [BloomFilter   for col N]           │  ← 等值查询加速
│    [ShortKeyIndex]                     │  ← 段级稀疏前缀索引
├────────────────────────────────────────┤
│  FOOTER                                │
│    SegmentFooter（自定义二进制元数据）   │
│    CRC32 checksum  (4 bytes)           │
│    Footer length   (4 bytes)           │
│    MAGIC           (8 bytes)           │
└────────────────────────────────────────┘
```

---

## 快速开始

### 依赖

```toml
[dependencies]
lz4       = "1"
crc32fast = "1"
byteorder = "1"
thiserror = "1"
```

### 运行示例

```bash
cargo run --example basic_usage
```

### 核心 API 速览

```rust
use olap_engine::{
    common::{AggregateType, ColumnType, KeysType, Version},
    meta::{ColumnSchema, RowsetMeta, TabletSchema},
    partition::{BucketType, MaterializedIndex, Partition, PartitionInfo,
                RangeBound, RangePartitionItem},
    storage::{PartitionSpec, StorageEngine},
    field_type::{ColumnMeta, EncodingType, FieldType, Value},
    segment::{SegmentReader, SegmentWriter},
};

// 1. 启动引擎
let engine = StorageEngine::new_single_dir("/tmp/olap-data");

// 2. 创建数据库 + 表（含分区和桶定义）
engine.create_database(1, "mydb")?;
engine.create_table_with_partitions(
    1, 100, "orders", schema, partition_info, specs, 1
)?;

// 3. 路由一行到 Tablet
let table = engine.catalog_manager.get_table(1, 100)?;
let tablet_id = table.read().unwrap().tablet_for_row("2024-05-15", "user_42")?;

// 4. 写 Segment V2
let mut writer = SegmentWriter::new(seg_schema);
writer.append_row(vec![Value::Int32(20240515), Value::Int64(42), ...])?;
let mut buf = Vec::new();
writer.finalize(&mut buf)?;

// 5. 读 Segment V2
let reader = SegmentReader::open(buf, seg_schema)?;
let values = reader.read_column(0)?;

// 6. 发布 Rowset
engine.publish_rowset(tablet_id, schema_hash, rowset_meta)?;

// 7. 查询版本图
let tablet  = engine.get_tablet(tablet_id, schema_hash)?;
let rowsets = tablet.capture_consistent_versions(0, 10)?;
```

---

## 关键设计决策

### 分片 Tablet 注册表
`TabletManager` 使用 64 个 `RwLock<Shard>` 降低并发读写竞争。

### Version Graph（版本 DAG）
每个 Tablet 维护一个有向图，边 `start → end` 代表覆盖 `[start,end]` 的 Rowset。
- BFS 检测版本空洞，O(V+E)
- BFS 找最小覆盖 Rowset 集合，支持 snapshot 读

### 不可变 Rowset
Rowset 一旦发布即不可修改；Compaction 生成新 Rowset 并将旧 Rowset 标记为 Stale 待 GC。

### 列编码与压缩组合
- 有序整数列（timestamp/id）→ `DeltaBinary + LZ4`，压缩比最高
- 低基数字符串列（status/country）→ `Dictionary + LZ4`，节省 60-80% 空间
- 其余 → `Plain + LZ4`

### ZoneMap 剪枝
每页记录 min/max，range 查询时直接跳过不相关页，无需解压。

---

## License

Apache 2.0
