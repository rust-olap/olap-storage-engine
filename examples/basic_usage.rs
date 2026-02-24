//! # olap-engine 完整使用案例
//!
//! 演示合并后项目的全部核心功能：
//!
//! 1. 启动 StorageEngine
//! 2. 创建数据库与分区表（RANGE 分区 + HASH 分桶）
//! 3. 路由行到 Tablet
//! 4. 写入 Segment V2 文件（ColumnWriter → SegmentWriter）
//! 5. 读取 Segment 文件并验证数据
//! 6. 发布 Rowset、查询版本图
//! 7. Compaction 调度演示

use olap_storage_engine::{
    // Storage 层
    common::{AggregateType, ColumnType, CompactionType, KeysType, Version},
    meta::{ColumnSchema, RowsetMeta, TabletMeta, TabletSchema},
    partition::{
        BucketType, MaterializedIndex, Partition, PartitionInfo,
        RangeBound, RangePartitionItem,
    },
    storage::{PartitionSpec, StorageEngine},
    table::OlapTable,
    // Segment 层
    field_type::{ColumnMeta, CompressionType, EncodingType, FieldType, Value},
    segment::SegmentWriter,
};

use std::collections::HashMap;

fn main() -> olap_storage_engine::common::Result<()> {
    println!("═══════════════════════════════════════════════════════════");
    println!("   olap-engine 合并项目演示                                ");
    println!("═══════════════════════════════════════════════════════════\n");

    // =========================================================================
    // 1. 启动 StorageEngine
    // =========================================================================
    println!("【1】启动 StorageEngine ...");
    let engine = StorageEngine::new_single_dir("/tmp/olap-data");
    println!("    data_dir = {}\n", engine.data_dir);

    // =========================================================================
    // 2. 创建数据库
    // =========================================================================
    println!("【2】创建数据库 `ecommerce` (db_id=1) ...");
    engine.create_database(1, "ecommerce")?;
    println!("    ✓ OK\n");

    // =========================================================================
    // 3. 定义 Schema（订单表）
    // =========================================================================
    println!("【3】定义表 Schema ...");
    // 表结构：orders (order_date DATE key, order_id BIGINT key, user_id BIGINT,
    //                  amount DOUBLE, status VARCHAR(32))
    let schema = TabletSchema::new(
        KeysType::Duplicate,
        vec![
            ColumnSchema::key(0, "order_date", ColumnType::Date),
            ColumnSchema::key(1, "order_id",   ColumnType::Int64),
            ColumnSchema::value(2, "user_id", ColumnType::Int64, AggregateType::None),
            ColumnSchema::value(3, "amount",  ColumnType::Float64, AggregateType::Sum),
            ColumnSchema::varchar(4, "status", 32, false),
        ],
    );
    println!("    schema_hash = {}", schema.schema_hash);
    println!("    columns     = {}", schema.num_columns());
    println!("    keys_type   = {:?}\n", schema.keys_type);

    // =========================================================================
    // 4. 创建 RANGE 分区 + HASH 分桶
    // =========================================================================
    //  分区：
    //    p_2024_h1: order_date < "2024-07-01"   → partition_id=10
    //    p_2024_h2: order_date < "2025-01-01"   → partition_id=11
    //  每个分区 4 桶，tablet_id 区间：100-103, 200-203
    println!("【4】创建 RANGE 分区表 `orders` ...");

    // --- 分区 10：2024 上半年 ---
    let p10_tablets = vec![100u64, 101, 102, 103];
    let p10_index   = MaterializedIndex::new(1, p10_tablets.clone());
    let p10_bucket  = BucketType::Hash {
        bucket_columns: vec!["order_id".into()],
        num_buckets: 4,
    };
    let p10 = Partition::new(10, p10_index, p10_bucket);

    // --- 分区 11：2024 下半年 ---
    let p11_tablets = vec![200u64, 201, 202, 203];
    let p11_index   = MaterializedIndex::new(2, p11_tablets.clone());
    let p11_bucket  = BucketType::Hash {
        bucket_columns: vec!["order_id".into()],
        num_buckets: 4,
    };
    let p11 = Partition::new(11, p11_index, p11_bucket);

    let mut partitions = HashMap::new();
    partitions.insert(10u64, p10);
    partitions.insert(11u64, p11);

    let partition_info = PartitionInfo::range(
        vec!["order_date".into()],
        vec![
            RangePartitionItem { partition_id: 10, upper_bound: RangeBound("2024-07-01".into()) },
            RangePartitionItem { partition_id: 11, upper_bound: RangeBound("2025-01-01".into()) },
        ],
        partitions,
    );

    // PartitionSpec 告诉 StorageEngine 要创建哪些 Tablet
    let specs = vec![
        PartitionSpec {
            partition_id: 10,
            tablet_ids:   p10_tablets.clone(),
            schema_hash:  schema.schema_hash,
        },
        PartitionSpec {
            partition_id: 11,
            tablet_ids:   p11_tablets.clone(),
            schema_hash:  schema.schema_hash,
        },
    ];

    engine.create_table_with_partitions(
        1,        // db_id
        1000,     // table_id
        "orders",
        schema.clone(),
        partition_info,
        specs,
        1,        // replication_num
    )?;

    println!("    ✓ 表创建完毕，总 Tablet 数 = {}\n", engine.tablet_count());

    // =========================================================================
    // 5. 行路由演示
    // =========================================================================
    println!("【5】行路由演示 ...");
    let table_arc = engine.catalog_manager.get_table(1, 1000)?;
    let table     = table_arc.read().unwrap();

    let test_rows = vec![
        ("2024-03-15", "1001001"),
        ("2024-03-15", "1001002"),
        ("2024-09-20", "2002001"),
        ("2024-11-11", "3003003"),
    ];
    for (date, order_id) in &test_rows {
        let tid = table.tablet_for_row(date, order_id)?;
        println!("    order_date={date}  order_id={order_id}  → tablet_id={tid}");
    }
    drop(table);
    println!();

    // =========================================================================
    // 6. Segment V2 写入演示
    // =========================================================================
    println!("【6】Segment V2 列存写入 ...");

    // Segment 层的列元数据（与 TabletSchema 对应）
    let seg_schema = vec![
        ColumnMeta::new(0, "order_date", FieldType::Int32)
            .with_encoding(EncodingType::DeltaBinary),
        ColumnMeta::new(1, "order_id",   FieldType::Int64)
            .with_encoding(EncodingType::DeltaBinary),
        ColumnMeta::new(2, "user_id",    FieldType::Int64)
            .with_encoding(EncodingType::DeltaBinary),
        ColumnMeta::new(3, "amount",     FieldType::Float64)
            .with_encoding(EncodingType::Plain),
        ColumnMeta::new(4, "status",     FieldType::Bytes)
            .with_encoding(EncodingType::Dictionary)
            .with_compression(CompressionType::Lz4)
            .nullable(),
    ];

    let mut seg_writer = SegmentWriter::new(seg_schema.clone());

    // 写入 2000 行模拟数据
    let statuses = ["pending", "paid", "shipped", "delivered", "cancelled"];
    for i in 0u32..2000 {
        let row = vec![
            Value::Int32(20240101 + (i as i32 % 180)),                   // order_date
            Value::Int64(1_000_000 + i as i64),                           // order_id
            Value::Int64(10000 + (i as i64 % 1000)),                      // user_id
            Value::Float64(99.9 + i as f64 * 0.5),                        // amount
            Value::Bytes(statuses[i as usize % 5].as_bytes().to_vec()),   // status
        ];
        seg_writer.append_row(row)?;
    }

    let num_rows = seg_writer.num_rows();
    let mut seg_bytes: Vec<u8> = Vec::new();
    let total_size = seg_writer.finalize(&mut seg_bytes)?;

    println!("    ✓ 写入行数     = {num_rows}");
    println!("    ✓ Segment 大小 = {} bytes ({:.1} KB)", total_size, total_size as f64 / 1024.0);
    println!();

    // =========================================================================
    // 7. Segment V2 读取演示
    // =========================================================================
    println!("【7】Segment V2 列存读取 ...");

    use olap_storage_engine::segment::SegmentReader;
    let reader = SegmentReader::open(seg_bytes, seg_schema)?;
    println!("    Segment 总行数 = {}", reader.num_rows());

    // 读取 order_id 列（Delta 编码）
    let order_ids = reader.read_column(1)?;
    println!("    order_id 列：前 5 值 = {:?}", &order_ids[..5.min(order_ids.len())]);

    // 读取 status 列（Dictionary 编码）
    let statuses_col = reader.read_column(4)?;
    println!("    status   列：前 5 值 = {:?}",
        &statuses_col[..5.min(statuses_col.len())].iter()
            .map(|v| v.to_string()).collect::<Vec<_>>());
    println!();

    // =========================================================================
    // 8. 发布 Rowset（模拟 Load 完成）
    // =========================================================================
    println!("【8】发布 Rowset ...");

    let schema_hash = schema.schema_hash;
    let tablet_id   = 100u64; // 分区 10 第 0 桶

    // 第一个 Rowset：版本 [0, 1]
    let rs1 = RowsetMeta::new(
        /*rowset_id=*/ 1, tablet_id, /*partition_id=*/ 10,
        Version::new(0, 1),
        /*num_rows=*/ 2000,
        /*disk_size=*/ total_size,
    );
    engine.publish_rowset(tablet_id, schema_hash, rs1)?;
    println!("    ✓ 发布 Rowset-1 [0,1]  tablet={tablet_id}");

    // 第二个 Rowset：版本 [2, 3]
    let rs2 = RowsetMeta::new(2, tablet_id, 10, Version::new(2, 3), 500, 50_000);
    engine.publish_rowset(tablet_id, schema_hash, rs2)?;
    println!("    ✓ 发布 Rowset-2 [2,3]  tablet={tablet_id}");

    // 查询覆盖版本 [0,3] 的 Rowset 集合
    let tablet  = engine.get_tablet(tablet_id, schema_hash)?;
    let rowsets = tablet.capture_consistent_versions(0, 3)?;
    println!("    覆盖 [0,3] 的 Rowset 集合（共 {} 个）：", rowsets.len());
    for rs in &rowsets {
        println!("      rowset_id={} version={} num_rows={}", rs.rowset_id, rs.version, rs.num_rows);
    }
    println!("    最新版本 = {}\n", tablet.max_version());

    // =========================================================================
    // 9. Compaction 调度演示
    // =========================================================================
    println!("【9】Compaction 调度 ...");

    // 再给 tablet 100 发布几个小 Rowset（提高 compaction 分数）
    for i in 3i64..8 {
        let rs = RowsetMeta::new(
            (10 + i) as u64, tablet_id, 10,
            Version::new(i * 2, i * 2 + 1), 100, 10_000,
        );
        let _ = engine.publish_rowset(tablet_id, schema_hash, rs);
    }

    let candidates = engine.schedule_compaction(CompactionType::Cumulative);
    println!("    Cumulative Compaction 候选 tablet (最多10个)：");
    for tid in &candidates {
        println!("      tablet_id={tid}");
    }
    println!();

    // =========================================================================
    // 10. 直接操作 VersionGraph
    // =========================================================================
    println!("【10】VersionGraph 分析 ...");
    let compaction_score = tablet.compute_compaction_score(CompactionType::Cumulative);
    println!("    tablet {} compaction score = {:.1}", tablet_id, compaction_score);

    // 检查版本连续性
    match tablet.capture_consistent_versions(0, 100) {
        Ok(_)  => println!("    版本 [0,100] 连续"),
        Err(e) => println!("    版本 [0,100] 不连续（预期）: {e}"),
    }

    println!("\n═══════════════════════════════════════════════════════════");
    println!("   全部演示完成 ✓");
    println!("═══════════════════════════════════════════════════════════");
    Ok(())
}
