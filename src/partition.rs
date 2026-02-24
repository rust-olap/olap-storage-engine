//! 分区与分桶策略

use std::collections::HashMap;
use crate::common::{OlapError, PartitionId, Result, TabletId};

// ── 分桶策略 ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum BucketType {
    /// HASH 分桶：对指定列做 FNV1a 哈希取模
    Hash {
        bucket_columns: Vec<String>,
        num_buckets:    u32,
    },
    /// RANDOM 分桶：写入时随机选桶
    Random { num_buckets: u32 },
}

impl BucketType {
    pub fn num_buckets(&self) -> u32 {
        match self {
            Self::Hash   { num_buckets, .. } => *num_buckets,
            Self::Random { num_buckets }     => *num_buckets,
        }
    }

    /// 将行键映射到桶索引
    pub fn bucket_for_key(&self, key: &str) -> u32 {
        match self {
            Self::Hash { num_buckets, .. } => {
                // FNV-1a 64-bit
                let mut h: u64 = 0xcbf29ce484222325;
                for b in key.bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                (h % *num_buckets as u64) as u32
            }
            Self::Random { num_buckets } => {
                let t = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos();
                t % num_buckets
            }
        }
    }
}

// ── MaterializedIndex（一个分区内一个索引的所有 Tablet）────────────────────────

#[derive(Debug, Clone)]
pub struct MaterializedIndex {
    pub index_id: u64,
    /// 长度 == num_buckets，tablets[bucket] = TabletId
    pub tablets:  Vec<TabletId>,
}

impl MaterializedIndex {
    pub fn new(index_id: u64, tablets: Vec<TabletId>) -> Self {
        Self { index_id, tablets }
    }

    pub fn tablet_for_bucket(&self, bucket: u32) -> Option<TabletId> {
        self.tablets.get(bucket as usize).copied()
    }
}

// ── Partition ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Partition {
    pub partition_id:    PartitionId,
    pub base_index:      MaterializedIndex,
    pub rollup_indexes:  Vec<MaterializedIndex>,
    pub bucket_type:     BucketType,
    /// 当前可见版本
    pub visible_version: i64,
}

impl Partition {
    pub fn new(
        partition_id: PartitionId,
        base_index:   MaterializedIndex,
        bucket_type:  BucketType,
    ) -> Self {
        Self {
            partition_id, base_index,
            rollup_indexes: vec![],
            bucket_type,
            visible_version: 0,
        }
    }

    /// 根据 sort_key 路由到 TabletId
    pub fn tablet_for_key(&self, sort_key: &str) -> Option<TabletId> {
        let bucket = self.bucket_type.bucket_for_key(sort_key);
        self.base_index.tablet_for_bucket(bucket)
    }
}

// ── Range 分区辅助 ─────────────────────────────────────────────────────────────

/// RANGE 分区边界（字符串比较）
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeBound(pub String);

impl RangeBound {
    /// 最大上界（超过所有实际数据）
    pub fn max_value() -> Self { Self("\u{FFFF}".repeat(64)) }
}

#[derive(Debug, Clone)]
pub struct RangePartitionItem {
    pub partition_id: PartitionId,
    /// 独占上界：key < upper_bound 则属于本分区
    pub upper_bound:  RangeBound,
}

// ── PartitionInfo ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PartitionPolicy {
    Range { items: Vec<RangePartitionItem> },
    List  { key_to_partition: HashMap<String, PartitionId> },
    Unpartitioned { partition_id: PartitionId },
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_columns: Vec<String>,
    pub policy:            PartitionPolicy,
    pub partitions:        HashMap<PartitionId, Partition>,
}

impl PartitionInfo {
    // ── 构造辅助 ──────────────────────────────────────────────────────────────

    pub fn range(
        partition_columns: Vec<String>,
        items:             Vec<RangePartitionItem>,
        partitions:        HashMap<PartitionId, Partition>,
    ) -> Self {
        Self { partition_columns, policy: PartitionPolicy::Range { items }, partitions }
    }

    pub fn list(
        partition_columns: Vec<String>,
        mapping:           HashMap<String, PartitionId>,
        partitions:        HashMap<PartitionId, Partition>,
    ) -> Self {
        Self {
            partition_columns,
            policy: PartitionPolicy::List { key_to_partition: mapping },
            partitions,
        }
    }

    pub fn unpartitioned(partition_id: PartitionId, partition: Partition) -> Self {
        let mut partitions = HashMap::new();
        partitions.insert(partition_id, partition);
        Self {
            partition_columns: vec![],
            policy: PartitionPolicy::Unpartitioned { partition_id },
            partitions,
        }
    }

    // ── 路由 ──────────────────────────────────────────────────────────────────

    /// 根据分区键值找到对应的 Partition
    pub fn find_partition(&self, key: &str) -> Result<&Partition> {
        let pid = match &self.policy {
            PartitionPolicy::Unpartitioned { partition_id } => *partition_id,

            PartitionPolicy::List { key_to_partition } =>
                *key_to_partition.get(key)
                    .ok_or_else(|| OlapError::PartitionNotFound(key.into()))?,

            PartitionPolicy::Range { items } =>
                items.iter()
                    .find(|it| key < it.upper_bound.0.as_str())
                    .map(|it| it.partition_id)
                    .ok_or_else(|| OlapError::PartitionNotFound(key.into()))?,
        };

        self.partitions.get(&pid)
            .ok_or_else(|| OlapError::PartitionNotFound(format!("pid={pid}")))
    }
}
