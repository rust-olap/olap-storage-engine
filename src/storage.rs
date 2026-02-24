//! 顶层存储协调器

use crate::common::{CompactionType, DbId, Result, RowsetId, SchemaHash, TabletId, TableId};
use crate::meta::{RowsetMeta, TabletMeta, TabletSchema};
use crate::partition::PartitionInfo;
use crate::table::{CatalogManager, OlapTable};
use crate::tablet::{Tablet, TabletManager};

/// 创建表时每个分区的规格
pub struct PartitionSpec {
    pub partition_id: crate::common::PartitionId,
    /// 每个桶对应一个 TabletId，len == num_buckets
    pub tablet_ids:   Vec<TabletId>,
    pub schema_hash:  SchemaHash,
}

/// 单节点 OLAP 存储引擎
pub struct StorageEngine {
    pub data_dir:        String,
    pub tablet_manager:  TabletManager,
    pub catalog_manager: CatalogManager,
}

impl StorageEngine {
    pub fn new_single_dir(data_dir: &str) -> Self {
        Self {
            data_dir:        data_dir.into(),
            tablet_manager:  TabletManager::new(),
            catalog_manager: CatalogManager::new(),
        }
    }

    // ── DDL ───────────────────────────────────────────────────────────────────

    pub fn create_database(&self, db_id: DbId, db_name: &str) -> Result<()> {
        self.catalog_manager.create_database(db_id, db_name)
    }

    /// 创建 Table 同时创建所有 Partition / Tablet
    pub fn create_table_with_partitions(
        &self,
        db_id:            DbId,
        table_id:         TableId,
        table_name:       &str,
        schema:           TabletSchema,
        partition_info:   PartitionInfo,
        partition_specs:  Vec<PartitionSpec>,
        _replication_num: u32,
    ) -> Result<()> {
        // 1. 为每个分区的每个桶创建 Tablet
        for spec in &partition_specs {
            for &tid in &spec.tablet_ids {
                let meta = TabletMeta::new(tid, spec.partition_id, schema.clone());
                self.tablet_manager.create_tablet(meta)?;
            }
        }
        // 2. 将 Table 注册进 Catalog
        let table = OlapTable::new(table_id, table_name, schema, partition_info);
        self.catalog_manager.add_table(db_id, table)
    }

    // ── 低级 Tablet 操作 ──────────────────────────────────────────────────────

    pub fn create_tablet(&self, meta: TabletMeta) -> Result<Tablet> {
        self.tablet_manager.create_tablet(meta)
    }

    pub fn get_tablet(&self, tablet_id: TabletId, schema_hash: SchemaHash) -> Result<Tablet> {
        self.tablet_manager.get_tablet(tablet_id, schema_hash)
    }

    pub fn drop_tablet(&self, tablet_id: TabletId, schema_hash: SchemaHash) -> Result<()> {
        self.tablet_manager.drop_tablet(tablet_id, schema_hash)
    }

    // ── Rowset 发布 ───────────────────────────────────────────────────────────

    /// 将一个已提交的 Rowset 发布到对应 Tablet（Load 完成后调用）
    pub fn publish_rowset(
        &self,
        tablet_id:   TabletId,
        schema_hash: SchemaHash,
        rowset:      RowsetMeta,
    ) -> Result<()> {
        let tablet = self.tablet_manager.get_tablet(tablet_id, schema_hash)?;
        tablet.add_rowset(rowset)
    }

    // ── Compaction 调度 ───────────────────────────────────────────────────────

    /// 调度一轮 Compaction，返回得分最高的 tablet_id 列表
    pub fn schedule_compaction(&self, ctype: CompactionType) -> Vec<TabletId> {
        self.tablet_manager
            .collect_compaction_candidates(ctype)
            .into_iter()
            .take(10)
            .map(|(tid, _, _)| tid)
            .collect()
    }

    // ── 辅助 ─────────────────────────────────────────────────────────────────

    pub fn tablet_count(&self) -> usize {
        self.tablet_manager.tablet_count()
    }

    /// 生成 Segment 文件在磁盘上的绝对路径
    pub fn segment_path(
        &self,
        tablet_id: TabletId,
        rowset_id: RowsetId,
        seg_idx:   u32,
    ) -> String {
        format!("{}/{}/{}_{}.seg", self.data_dir, tablet_id, rowset_id, seg_idx)
    }
}
