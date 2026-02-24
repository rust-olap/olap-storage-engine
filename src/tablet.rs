//! Tablet 运行时状态（Version Graph + Tablet 句柄 + TabletManager）

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use crate::common::{
    CompactionType, OlapError, Result, SchemaHash, TabletId, Version,
};
use crate::meta::{RowsetMeta, RowsetState, TabletMeta, TabletSchema};

// ── Version 图 ────────────────────────────────────────────────────────────────
//
// 有向图：每条边 start → end 代表一个覆盖 [start,end] 的 Rowset。
// 支持：
//   • 检测版本空洞（O(V+E) BFS）
//   • 找出覆盖 [lo,hi] 的最小 Rowset 集合

#[derive(Debug, Default)]
pub struct VersionGraph {
    // start_version → { end_version, ... }
    adj: HashMap<i64, HashSet<i64>>,
}

impl VersionGraph {
    pub fn add_edge(&mut self, v: Version) {
        self.adj.entry(v.start).or_default().insert(v.end);
    }

    pub fn remove_edge(&mut self, v: Version) {
        if let Some(ends) = self.adj.get_mut(&v.start) {
            ends.remove(&v.end);
            if ends.is_empty() {
                self.adj.remove(&v.start);
            }
        }
    }

    /// BFS 找从 lo 到 hi 的覆盖路径，返回经过的 Version 列表。
    pub fn find_covering_path(&self, lo: i64, hi: i64) -> Option<Vec<Version>> {
        let mut queue: VecDeque<(i64, Vec<Version>)> = VecDeque::new();
        queue.push_back((lo, vec![]));
        let mut visited: HashSet<i64> = HashSet::new();
        visited.insert(lo);

        while let Some((cur, path)) = queue.pop_front() {
            if let Some(ends) = self.adj.get(&cur) {
                // 优先尝试跨度最大的边
                let mut sorted: Vec<i64> = ends.iter().copied().collect();
                sorted.sort_unstable_by(|a, b| b.cmp(a));

                for &end in &sorted {
                    let mut new_path = path.clone();
                    new_path.push(Version::new(cur, end));

                    if end == hi {
                        return Some(new_path);
                    }
                    if end < hi && !visited.contains(&(end + 1)) {
                        visited.insert(end + 1);
                        queue.push_back((end + 1, new_path));
                    }
                }
            }
        }
        None
    }

    pub fn has_version_holes(&self, lo: i64, hi: i64) -> bool {
        self.find_covering_path(lo, hi).is_none()
    }
}

// ── Tablet 内部状态 ───────────────────────────────────────────────────────────

pub struct TabletInner {
    pub meta:          TabletMeta,
    pub version_graph: VersionGraph,
}

impl TabletInner {
    fn new(meta: TabletMeta) -> Self {
        let mut vg = VersionGraph::default();
        for rs in meta.rowsets.values() {
            vg.add_edge(rs.version);
        }
        Self { meta, version_graph: vg }
    }
}

// ── Tablet 句柄 ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Tablet(Arc<RwLock<TabletInner>>);

impl Tablet {
    pub fn new(meta: TabletMeta) -> Self {
        Self(Arc::new(RwLock::new(TabletInner::new(meta))))
    }

    pub fn tablet_id(&self) -> TabletId {
        self.0.read().unwrap().meta.tablet_id
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.0.read().unwrap().meta.schema_hash
    }

    pub fn schema(&self) -> TabletSchema {
        self.0.read().unwrap().meta.schema.clone()
    }

    pub fn max_version(&self) -> i64 {
        self.0.read().unwrap().meta.max_version
    }

    /// 将一个已提交的 Rowset 发布到本 Tablet
    pub fn add_rowset(&self, mut rs: RowsetMeta) -> Result<()> {
        let mut inner = self.0.write().unwrap();
        if inner.meta.rowsets.contains_key(&rs.rowset_id) {
            return Err(OlapError::VersionExists(rs.version));
        }
        rs.state = RowsetState::Visible;
        inner.version_graph.add_edge(rs.version);
        if rs.version.end > inner.meta.max_version {
            inner.meta.max_version = rs.version.end;
        }
        inner.meta.rowsets.insert(rs.rowset_id, rs);
        Ok(())
    }

    /// 找出覆盖 [lo, hi] 版本范围的最小 Rowset 元数据集合
    pub fn capture_consistent_versions(&self, lo: i64, hi: i64) -> Result<Vec<RowsetMeta>> {
        let inner = self.0.read().unwrap();
        let path = inner.version_graph
            .find_covering_path(lo, hi)
            .ok_or_else(|| OlapError::MissingVersions(format!("[{lo},{hi}]")))?;

        let rowsets = path
            .iter()
            .filter_map(|v| {
                inner.meta.rowsets.values().find(|r| r.version == *v).cloned()
            })
            .collect();
        Ok(rowsets)
    }

    /// Compaction 优先级得分（可见 Rowset 数量）
    pub fn compute_compaction_score(&self, _ctype: CompactionType) -> f64 {
        let inner = self.0.read().unwrap();
        inner.meta.rowsets.values()
            .filter(|r| r.state == RowsetState::Visible)
            .count() as f64
    }

    /// 将指定 Rowset 标记为 Stale（compaction 后调用）
    pub fn mark_rowset_stale(&self, rowset_id: u64) {
        let mut inner = self.0.write().unwrap();
        let version = match inner.meta.rowsets.get_mut(&rowset_id) {
            Some(rs) => {
                rs.mark_stale();
                rs.version
            }
            None => return,
        };
        inner.version_graph.remove_edge(version);
    }
}

// ── Tablet 分片注册表 ─────────────────────────────────────────────────────────

const NUM_SHARDS: usize = 64;

struct Shard {
    tablets: HashMap<(TabletId, SchemaHash), Tablet>,
}

/// 分片 Tablet 注册表
///
/// 使用 64 个 RwLock Shard 降低读写竞争。
pub struct TabletManager {
    shards: Vec<RwLock<Shard>>,
}

impl TabletManager {
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| RwLock::new(Shard { tablets: HashMap::new() }))
            .collect();
        Self { shards }
    }

    fn shard(&self, tablet_id: TabletId) -> &RwLock<Shard> {
        &self.shards[(tablet_id as usize) % NUM_SHARDS]
    }

    pub fn create_tablet(&self, meta: TabletMeta) -> Result<Tablet> {
        let key = (meta.tablet_id, meta.schema_hash);
        let tablet = Tablet::new(meta);
        self.shard(key.0).write().unwrap().tablets.insert(key, tablet.clone());
        Ok(tablet)
    }

    pub fn get_tablet(&self, tablet_id: TabletId, schema_hash: SchemaHash) -> Result<Tablet> {
        self.shard(tablet_id)
            .read().unwrap()
            .tablets.get(&(tablet_id, schema_hash))
            .cloned()
            .ok_or(OlapError::TabletNotFound(tablet_id))
    }

    pub fn drop_tablet(&self, tablet_id: TabletId, schema_hash: SchemaHash) -> Result<()> {
        self.shard(tablet_id)
            .write().unwrap()
            .tablets.remove(&(tablet_id, schema_hash))
            .map(|_| ())
            .ok_or(OlapError::TabletNotFound(tablet_id))
    }

    pub fn tablet_count(&self) -> usize {
        self.shards.iter().map(|s| s.read().unwrap().tablets.len()).sum()
    }

    /// 遍历所有 Tablet，返回 (tablet_id, schema_hash, compaction_score)
    pub fn collect_compaction_candidates(
        &self, ctype: CompactionType,
    ) -> Vec<(TabletId, SchemaHash, f64)> {
        let mut result = Vec::new();
        for shard in &self.shards {
            let guard = shard.read().unwrap();
            for ((tid, shash), tablet) in &guard.tablets {
                let score = tablet.compute_compaction_score(ctype);
                result.push((*tid, *shash, score));
            }
        }
        result.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());
        result
    }
}

impl Default for TabletManager {
    fn default() -> Self { Self::new() }
}
