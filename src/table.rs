//! Table 元数据与 Catalog

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::common::{DbId, OlapError, Result, TableId, TabletId};
use crate::meta::TabletSchema;
use crate::partition::PartitionInfo;

// ── OlapTable ─────────────────────────────────────────────────────────────────

pub struct OlapTable {
    pub table_id:       TableId,
    pub table_name:     String,
    pub schema:         TabletSchema,
    pub partition_info: PartitionInfo,
}

impl OlapTable {
    pub fn new(
        table_id:       TableId,
        table_name:     &str,
        schema:         TabletSchema,
        partition_info: PartitionInfo,
    ) -> Self {
        Self { table_id, table_name: table_name.into(), schema, partition_info }
    }

    /// 将行路由到对应的 TabletId（分区路由 + 桶路由）
    ///
    /// - `partition_key`：参与分区判断的列值（如日期字符串）
    /// - `sort_key`      ：参与分桶哈希的列值（如 user_id）
    pub fn tablet_for_row(
        &self,
        partition_key: &str,
        sort_key:      &str,
    ) -> Result<TabletId> {
        let partition = self.partition_info.find_partition(partition_key)?;
        partition
            .tablet_for_key(sort_key)
            .ok_or_else(|| OlapError::PartitionNotFound(sort_key.into()))
    }
}

// ── Database ──────────────────────────────────────────────────────────────────

struct Database {
    _db_id:   DbId,
    _db_name: String,
    tables:   HashMap<TableId, Arc<RwLock<OlapTable>>>,
}

// ── CatalogManager ────────────────────────────────────────────────────────────

/// 线程安全的 Catalog
pub struct CatalogManager {
    databases: RwLock<HashMap<DbId, Database>>,
}

impl CatalogManager {
    pub fn new() -> Self {
        Self { databases: RwLock::new(HashMap::new()) }
    }

    pub fn create_database(&self, db_id: DbId, db_name: &str) -> Result<()> {
        self.databases.write().unwrap().insert(
            db_id,
            Database { _db_id: db_id, _db_name: db_name.into(), tables: HashMap::new() },
        );
        Ok(())
    }

    pub fn add_table(&self, db_id: DbId, table: OlapTable) -> Result<()> {
        let mut dbs = self.databases.write().unwrap();
        let db = dbs.get_mut(&db_id).ok_or(OlapError::DatabaseNotFound(db_id))?;
        db.tables.insert(table.table_id, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn get_table(
        &self,
        db_id:    DbId,
        table_id: TableId,
    ) -> Result<Arc<RwLock<OlapTable>>> {
        let dbs = self.databases.read().unwrap();
        let db  = dbs.get(&db_id).ok_or(OlapError::DatabaseNotFound(db_id))?;
        db.tables.get(&table_id)
            .cloned()
            .ok_or(OlapError::TableNotFound(db_id, table_id))
    }

    pub fn drop_table(&self, db_id: DbId, table_id: TableId) -> Result<()> {
        let mut dbs = self.databases.write().unwrap();
        let db = dbs.get_mut(&db_id).ok_or(OlapError::DatabaseNotFound(db_id))?;
        db.tables.remove(&table_id)
            .map(|_| ())
            .ok_or(OlapError::TableNotFound(db_id, table_id))
    }
}

impl Default for CatalogManager {
    fn default() -> Self { Self::new() }
}
