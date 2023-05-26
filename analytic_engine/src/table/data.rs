// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table data

use std::{
    collections::HashMap,
    convert::TryInto,
    fmt,
    fmt::Formatter,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use arena::CollectorRef;
use common_types::{
    self,
    schema::{Schema, Version},
    table::ShardId,
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use common_util::define_result;
use log::{debug, info};
use object_store::Path;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::table::TableId;

use crate::{
    instance::serial_executor::TableOpSerialExecutor,
    manifest::meta_edit::AddTableMeta,
    memtable::{
        factory::{FactoryRef as MemTableFactoryRef, Options as MemTableOptions},
        skiplist::factory::SkiplistMemTableFactory,
    },
    space::SpaceId,
    sst::{file::FilePurger, manager::FileId},
    table::{
        metrics::Metrics,
        sst_util,
        version::{MemTableForWrite, MemTableState, SamplingMemTable, TableVersion},
    },
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create memtable, err:{}", source))]
    CreateMemTable {
        source: crate::memtable::factory::Error,
    },

    #[snafu(display(
        "Failed to find or create memtable, timestamp overflow, timestamp:{:?}, duration:{:?}.\nBacktrace:\n{}",
        timestamp,
        duration,
        backtrace,
    ))]
    TimestampOverflow {
        timestamp: Timestamp,
        duration: Duration,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find memtable for write, err:{}", source))]
    FindMemTable {
        source: crate::table::version::Error,
    },
}

define_result!(Error);

pub type MemTableId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableShardInfo {
    pub shard_id: ShardId,
}

impl TableShardInfo {
    pub fn new(shard_id: ShardId) -> Self {
        Self { shard_id }
    }
}

/// Data of a table
pub struct TableData {
    /// Id of this table
    pub id: TableId,
    /// Name of this table
    pub name: String,
    /// Schema of this table
    schema: Mutex<Schema>,
    /// Space id of this table
    pub space_id: SpaceId,

    /// Mutable memtable memory size limitation
    mutable_limit: AtomicU32,
    /// Mutable memtable memory usage ratio of the write buffer size.
    mutable_limit_write_buffer_ratio: f32,

    /// Options of this table
    ///
    /// Most modification to `opts` can be done by replacing the old options
    /// with a new one. However, altering the segment duration should be done
    /// carefully to avoid the reader seeing inconsistent segment duration
    /// and memtables/ssts during query/compaction/flush .
    opts: ArcSwap<TableOptions>,
    /// MemTable factory of this table
    memtable_factory: MemTableFactoryRef,
    /// Space memtable memory usage collector
    mem_usage_collector: CollectorRef,

    /// Current table version
    current_version: TableVersion,
    /// Last sequence visible to the reads
    ///
    /// Write to last_sequence should be guarded by a mutex and only done by
    /// single writer, but reads are allowed to be done concurrently without
    /// mutex protected
    last_sequence: AtomicU64,

    /// Auto incremented id to track memtable, reset on engine open
    ///
    /// Allocating memtable id should be guarded by write lock
    last_memtable_id: AtomicU64,

    /// Last id of the sst file
    ///
    /// Write to last_file_id require external synchronization
    last_file_id: AtomicU64,

    /// Last flush time
    ///
    /// Not persist, used to determine if this table should flush.
    last_flush_time_ms: AtomicU64,

    /// Flag denoting whether the table is dropped
    ///
    /// No write/alter is allowed if the table is dropped.
    dropped: AtomicBool,

    /// Metrics of this table
    pub metrics: Metrics,

    /// Shard info of the table
    pub shard_info: TableShardInfo,

    /// The table operation serial_exec
    serial_exec: tokio::sync::Mutex<TableOpSerialExecutor>,
}

impl fmt::Debug for TableData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableData")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("space", &self.space_id)
            .field("mutable_limit", &self.mutable_limit)
            .field("opts", &self.opts)
            .field("last_sequence", &self.last_sequence)
            .field("last_memtable_id", &self.last_memtable_id)
            .field("last_file_id", &self.last_file_id)
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .field("shard_info", &self.shard_info)
            .finish()
    }
}

impl Drop for TableData {
    fn drop(&mut self) {
        debug!("TableData is dropped, id:{}, name:{}", self.id, self.name);
    }
}

#[inline]
fn compute_mutable_limit(
    write_buffer_size: u32,
    mutable_limit_write_buffer_size_ratio: f32,
) -> u32 {
    assert!((0.0..=1.0).contains(&mutable_limit_write_buffer_size_ratio));

    let limit = write_buffer_size as f32 * mutable_limit_write_buffer_size_ratio;
    // This is safe because the limit won't be larger than the write_buffer_size.
    limit as u32
}

impl TableData {
    /// Create a new TableData
    ///
    /// This function should only be called when a new table is creating and
    /// there is no existing data of the table
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        space_id: SpaceId,
        table_id: TableId,
        table_name: String,
        table_schema: Schema,
        shard_id: ShardId,
        table_opts: TableOptions,
        purger: &FilePurger,
        preflush_write_buffer_size_ratio: f32,
        mem_usage_collector: CollectorRef,
    ) -> Result<Self> {
        // FIXME(yingwen): Validate TableOptions, such as bucket_duration >=
        // segment_duration and bucket_duration is aligned to segment_duration

        let memtable_factory = Arc::new(SkiplistMemTableFactory);
        let purge_queue = purger.create_purge_queue(space_id, table_id);
        let current_version = TableVersion::new(purge_queue);
        let metrics = Metrics::default();
        let mutable_limit = AtomicU32::new(compute_mutable_limit(
            table_opts.write_buffer_size,
            preflush_write_buffer_size_ratio,
        ));

        Ok(Self {
            id: table_id,
            name: table_name,
            schema: Mutex::new(table_schema),
            space_id,
            mutable_limit,
            mutable_limit_write_buffer_ratio: preflush_write_buffer_size_ratio,
            opts: ArcSwap::new(Arc::new(table_opts)),
            memtable_factory,
            mem_usage_collector,
            current_version,
            last_sequence: AtomicU64::new(0),
            last_memtable_id: AtomicU64::new(0),
            last_file_id: AtomicU64::new(0),
            last_flush_time_ms: AtomicU64::new(0),
            dropped: AtomicBool::new(false),
            metrics,
            shard_info: TableShardInfo::new(shard_id),
            serial_exec: tokio::sync::Mutex::new(TableOpSerialExecutor::new(table_id)),
        })
    }

    pub async fn acquire_serial_exec_lock(
        &self,
        reason: &str,
    ) -> tokio::sync::MutexGuard<TableOpSerialExecutor> {
        info!("Begin acquire lock of table:{}, reason:{reason}", self.name);
        let guard = self.serial_exec.lock().await;
        info!("Acquired lock of table:{}, reason:{reason}", self.name);

        guard
    }

    pub fn try_acquire_serial_exec_lock(
        &self,
        reason: &str,
    ) -> Option<tokio::sync::MutexGuard<TableOpSerialExecutor>> {
        info!(
            "Begin try acquire lock of table:{}, reason:{reason}",
            self.name
        );
        let guard = self.serial_exec.try_lock().map(|v| Some(v)).unwrap_or(None);
        info!("Acquired try lock of table:{}, reason:{reason}", self.name);

        guard
    }

    /// Recover table from add table meta
    ///
    /// This wont recover sequence number, which will be set after wal replayed
    pub fn recover_from_add(
        add_meta: AddTableMeta,
        purger: &FilePurger,
        shard_id: ShardId,
        preflush_write_buffer_size_ratio: f32,
        mem_usage_collector: CollectorRef,
    ) -> Result<Self> {
        let memtable_factory = Arc::new(SkiplistMemTableFactory);
        let purge_queue = purger.create_purge_queue(add_meta.space_id, add_meta.table_id);
        let current_version = TableVersion::new(purge_queue);
        let metrics = Metrics::default();
        let mutable_limit = AtomicU32::new(compute_mutable_limit(
            add_meta.opts.write_buffer_size,
            preflush_write_buffer_size_ratio,
        ));

        Ok(Self {
            id: add_meta.table_id,
            name: add_meta.table_name,
            schema: Mutex::new(add_meta.schema),
            space_id: add_meta.space_id,
            mutable_limit,
            mutable_limit_write_buffer_ratio: preflush_write_buffer_size_ratio,
            opts: ArcSwap::new(Arc::new(add_meta.opts)),
            memtable_factory,
            mem_usage_collector,
            current_version,
            last_sequence: AtomicU64::new(0),
            last_memtable_id: AtomicU64::new(0),
            last_file_id: AtomicU64::new(0),
            last_flush_time_ms: AtomicU64::new(0),
            dropped: AtomicBool::new(false),
            metrics,
            shard_info: TableShardInfo::new(shard_id),
            serial_exec: tokio::sync::Mutex::new(TableOpSerialExecutor::new(add_meta.table_id)),
        })
    }

    /// Get current schema of the table.
    pub fn schema(&self) -> Schema {
        self.schema.lock().unwrap().clone()
    }

    /// Set current schema of the table.
    pub fn set_schema(&self, schema: Schema) {
        *self.schema.lock().unwrap() = schema;
    }

    /// Get current version of schema.
    pub fn schema_version(&self) -> Version {
        self.schema.lock().unwrap().version()
    }

    /// Get current table version
    #[inline]
    pub fn current_version(&self) -> &TableVersion {
        &self.current_version
    }

    /// Get last sequence number
    #[inline]
    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(Ordering::Acquire)
    }

    /// Set last sequence number
    #[inline]
    pub fn set_last_sequence(&self, seq: SequenceNumber) {
        self.last_sequence.store(seq, Ordering::Release);
    }

    /// Get last flush time
    #[inline]
    pub fn last_flush_time(&self) -> u64 {
        self.last_flush_time_ms.load(Ordering::Relaxed)
    }

    /// Set last flush time
    #[inline]
    pub fn set_last_flush_time(&self, time: u64) {
        self.last_flush_time_ms.store(time, Ordering::Release);
    }

    #[inline]
    pub fn table_options(&self) -> Arc<TableOptions> {
        self.opts.load().clone()
    }

    /// Update table options.
    #[inline]
    pub fn set_table_options(&self, opts: TableOptions) {
        let mutable_limit = compute_mutable_limit(
            opts.write_buffer_size,
            self.mutable_limit_write_buffer_ratio,
        );
        self.mutable_limit.store(mutable_limit, Ordering::Relaxed);
        self.opts.store(Arc::new(opts))
    }

    #[inline]
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Set the table is dropped and forbid any writes/alter on this table.
    #[inline]
    pub fn set_dropped(&self) {
        self.dropped.store(true, Ordering::SeqCst);
    }

    /// Returns total memtable memory usage in bytes.
    #[inline]
    pub fn memtable_memory_usage(&self) -> usize {
        self.current_version.total_memory_usage()
    }

    /// Find memtable for given timestamp to insert, create if not exists
    ///
    /// If the memtable schema is outdated, switch all memtables and create the
    /// needed mutable memtable by current schema. The returned memtable is
    /// guaranteed to have same schema of current table
    pub fn find_or_create_mutable(
        &self,
        timestamp: Timestamp,
        table_schema: &Schema,
    ) -> Result<MemTableForWrite> {
        let schema_version = table_schema.version();
        let last_sequence = self.last_sequence();

        if let Some(mem) = self
            .current_version
            .memtable_for_write(timestamp, schema_version)
            .context(FindMemTable)?
        {
            return Ok(mem);
        }

        // Mutable memtable for this timestamp not found, need to create a new one.
        let table_options = self.table_options();
        let memtable_opts = MemTableOptions {
            schema: table_schema.clone(),
            arena_block_size: table_options.arena_block_size,
            creation_sequence: last_sequence,
            collector: self.mem_usage_collector.clone(),
        };
        let mem = self
            .memtable_factory
            .create_memtable(memtable_opts)
            .context(CreateMemTable)?;

        match table_options.segment_duration() {
            Some(segment_duration) => {
                let time_range = TimeRange::bucket_of(timestamp, segment_duration).context(
                    TimestampOverflow {
                        timestamp,
                        duration: segment_duration,
                    },
                )?;
                let mem_state = MemTableState {
                    mem,
                    time_range,
                    id: self.alloc_memtable_id(),
                };

                // Insert memtable into mutable memtables of current version.
                self.current_version.insert_mutable(mem_state.clone());

                Ok(MemTableForWrite::Normal(mem_state))
            }
            None => {
                let sampling_mem = SamplingMemTable::new(mem, self.alloc_memtable_id());
                debug!(
                    "create sampling mem table:{}, schema:{:#?}",
                    sampling_mem.id, table_schema
                );
                // Set sampling memtables of current version.
                self.current_version.set_sampling(sampling_mem.clone());

                Ok(MemTableForWrite::Sampling(sampling_mem))
            }
        }
    }

    /// Returns true if the memory usage of this table reaches flush threshold
    ///
    /// REQUIRE: Do in write worker
    pub fn should_flush_table(&self, serial_exec: &mut TableOpSerialExecutor) -> bool {
        // Fallback to usize::MAX if Failed to convert arena_block_size into
        // usize (overflow)
        let max_write_buffer_size = self
            .table_options()
            .write_buffer_size
            .try_into()
            .unwrap_or(usize::MAX);
        let mutable_limit = self
            .mutable_limit
            .load(Ordering::Relaxed)
            .try_into()
            .unwrap_or(usize::MAX);

        let mutable_usage = self.current_version.mutable_memory_usage();
        let total_usage = self.current_version.total_memory_usage();

        let in_flush = serial_exec.flush_scheduler().is_in_flush();
        // Inspired by https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L94
        if mutable_usage > mutable_limit && !in_flush {
            info!(
                "TableData should flush, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
                self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
            );
            return true;
        }

        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        let should_flush =
            total_usage >= max_write_buffer_size && mutable_usage >= max_write_buffer_size / 2;

        debug!(
            "Check should flush, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
            self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
        );

        if should_flush {
            info!(
                "TableData should flush, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
                self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
            );
        }

        should_flush
    }

    /// Set `last_file_id`, mainly used in recover
    ///
    /// This operation require external synchronization
    pub fn set_last_file_id(&self, last_file_id: FileId) {
        self.last_file_id.store(last_file_id, Ordering::Relaxed);
    }

    /// Returns the last file id
    pub fn last_file_id(&self) -> FileId {
        self.last_file_id.load(Ordering::Relaxed)
    }

    /// Alloc a file id for a new file
    pub fn alloc_file_id(&self) -> FileId {
        let last = self.last_file_id.fetch_add(1, Ordering::Relaxed);
        last + 1
    }

    /// Set the sst file path into the object storage path.
    pub fn set_sst_file_path(&self, file_id: FileId) -> Path {
        sst_util::new_sst_file_path(self.space_id, self.id, file_id)
    }

    /// Allocate next memtable id
    fn alloc_memtable_id(&self) -> MemTableId {
        let last = self.last_memtable_id.fetch_add(1, Ordering::Relaxed);
        last + 1
    }

    /// Returns last memtable id
    pub fn last_memtable_id(&self) -> MemTableId {
        self.last_memtable_id.load(Ordering::Relaxed)
    }

    pub fn dedup(&self) -> bool {
        self.table_options().need_dedup()
    }

    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        self.table_options().is_expired(timestamp)
    }

    pub fn table_location(&self) -> TableLocation {
        TableLocation {
            id: self.id.as_u64(),
            shard_info: self.shard_info,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableLocation {
    pub id: common_types::table::TableId,
    pub shard_info: TableShardInfo,
}

/// Table data reference
pub type TableDataRef = Arc<TableData>;

/// Manages TableDataRef
#[derive(Debug, Default)]
pub struct TableDataSet {
    /// Name to table data
    table_datas: HashMap<String, TableDataRef>,
    /// Id to table data
    id_to_tables: HashMap<TableId, TableDataRef>,
}

impl TableDataSet {
    /// Create an empty TableDataSet
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert if absent, if successfully inserted, return true and return
    /// false if the data already exists
    pub fn insert_if_absent(&mut self, table_data_ref: TableDataRef) -> bool {
        let table_name = &table_data_ref.name;
        if self.table_datas.contains_key(table_name) {
            return false;
        }
        self.table_datas
            .insert(table_name.to_string(), table_data_ref.clone());
        self.id_to_tables.insert(table_data_ref.id, table_data_ref);
        true
    }

    /// Find table by table name
    pub fn find_table(&self, table_name: &str) -> Option<TableDataRef> {
        self.table_datas.get(table_name).cloned()
    }

    /// Find table by table id
    pub fn find_table_by_id(&self, table_id: TableId) -> Option<TableDataRef> {
        self.id_to_tables.get(&table_id).cloned()
    }

    /// Remove table by table name
    pub fn remove_table(&mut self, table_name: &str) -> Option<TableDataRef> {
        let table = self.table_datas.remove(table_name)?;
        self.id_to_tables.remove(&table.id);
        Some(table)
    }

    /// Returns the total table num in this set
    pub fn table_num(&self) -> usize {
        self.table_datas.len()
    }

    pub fn find_maximum_memory_usage_table(&self) -> Option<TableDataRef> {
        // TODO: Possible performance issue here when there are too many tables.
        self.table_datas
            .values()
            .max_by_key(|t| t.memtable_memory_usage())
            .cloned()
    }

    /// List all tables to `tables`
    pub fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        for table_data in self.table_datas.values().cloned() {
            tables.push(table_data);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arena::NoopCollector;
    use common_types::{datum::DatumKind, table::DEFAULT_SHARD_ID};
    use common_util::config::ReadableDuration;
    use table_engine::{
        engine::{CreateTableRequest, TableState},
        table::SchemaId,
    };

    use super::*;
    use crate::{
        memtable::{factory::Factory, MemTableRef},
        sst::file::tests::FilePurgerMocker,
        table_options,
        tests::table,
    };

    const DEFAULT_SPACE_ID: SpaceId = 1;

    fn default_schema() -> Schema {
        table::create_schema_builder(
            &[("key", DatumKind::Timestamp)],
            &[("value", DatumKind::Double)],
        )
        .build()
        .unwrap()
    }

    #[derive(Default)]
    pub struct MemTableMocker;

    impl MemTableMocker {
        pub fn build(&self) -> MemTableRef {
            let memtable_opts = MemTableOptions {
                schema: default_schema(),
                arena_block_size: 1024 * 1024,
                creation_sequence: 1000,
                collector: Arc::new(NoopCollector),
            };

            let factory = SkiplistMemTableFactory;
            factory.create_memtable(memtable_opts).unwrap()
        }
    }

    #[must_use]
    pub struct TableDataMocker {
        table_id: TableId,
        table_name: String,
        shard_id: ShardId,
    }

    impl TableDataMocker {
        pub fn table_id(mut self, table_id: TableId) -> Self {
            self.table_id = table_id;
            self
        }

        pub fn table_name(mut self, table_name: String) -> Self {
            self.table_name = table_name;
            self
        }

        pub fn shard_id(mut self, shard_id: ShardId) -> Self {
            self.shard_id = shard_id;
            self
        }

        pub fn build(self) -> TableData {
            let space_id = DEFAULT_SPACE_ID;
            let table_schema = default_schema();
            let create_request = CreateTableRequest {
                catalog_name: "test_catalog".to_string(),
                schema_name: "public".to_string(),
                schema_id: SchemaId::from_u32(DEFAULT_SPACE_ID),
                table_id: self.table_id,
                table_name: self.table_name,
                table_schema,
                engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                options: HashMap::new(),
                state: TableState::Stable,
                shard_id: self.shard_id,
                partition_info: None,
            };

            let table_opts = TableOptions::default();
            let purger = FilePurgerMocker::mock();
            let collector = Arc::new(NoopCollector);

            TableData::new(
                space_id,
                create_request.table_id,
                create_request.table_name,
                create_request.table_schema,
                create_request.shard_id,
                table_opts,
                &purger,
                0.75,
                collector,
            )
            .unwrap()
        }
    }

    impl Default for TableDataMocker {
        fn default() -> Self {
            Self {
                table_id: table::new_table_id(2, 1),
                table_name: "mocked_table".to_string(),
                shard_id: DEFAULT_SHARD_ID,
            }
        }
    }

    #[test]
    fn test_new_table_data() {
        let table_id = table::new_table_id(100, 30);
        let table_name = "new_table".to_string();
        let shard_id = 42;
        let table_data = TableDataMocker::default()
            .table_id(table_id)
            .table_name(table_name.clone())
            .shard_id(shard_id)
            .build();

        assert_eq!(table_id, table_data.id);
        assert_eq!(table_name, table_data.name);
        assert_eq!(TableShardInfo::new(shard_id), table_data.shard_info);
        assert_eq!(0, table_data.last_sequence());
        assert!(!table_data.is_dropped());
        assert_eq!(0, table_data.last_file_id());
        assert_eq!(0, table_data.last_memtable_id());
        assert!(table_data.dedup());
    }

    #[test]
    fn test_find_or_create_mutable() {
        let table_data = TableDataMocker::default().build();
        let schema = table_data.schema();

        // Create sampling memtable.
        let zero_ts = Timestamp::new(0);
        let mutable = table_data.find_or_create_mutable(zero_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(zero_ts));
        let sampling_mem = mutable.as_sampling();
        let sampling_id = sampling_mem.id;
        assert_eq!(1, sampling_id);

        // Test memtable is reused.
        let now_ts = Timestamp::now();
        let mutable = table_data.find_or_create_mutable(now_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(now_ts));
        let sampling_mem = mutable.as_sampling();
        // Use same sampling memtable.
        assert_eq!(sampling_id, sampling_mem.id);

        let current_version = table_data.current_version();
        // Set segment duration manually.
        let mut table_opts = (*table_data.table_options()).clone();
        table_opts.segment_duration =
            Some(ReadableDuration(table_options::DEFAULT_SEGMENT_DURATION));
        table_data.set_table_options(table_opts);
        // Freeze sampling memtable.
        current_version.freeze_sampling();

        // A new mutable memtable should be created.
        let mutable = table_data.find_or_create_mutable(now_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(now_ts));
        let mem_state = mutable.as_normal();
        assert_eq!(2, mem_state.id);
        let time_range =
            TimeRange::bucket_of(now_ts, table_options::DEFAULT_SEGMENT_DURATION).unwrap();
        assert_eq!(time_range, mem_state.time_range);
    }

    #[test]
    fn test_compute_mutable_limit() {
        // Build the cases for compute_mutable_limit.
        let cases = vec![
            (80, 0.8, 64),
            (80, 0.5, 40),
            (80, 0.1, 8),
            (80, 0.0, 0),
            (80, 1.0, 80),
            (0, 0.8, 0),
            (0, 0.5, 0),
            (0, 0.1, 0),
            (0, 0.0, 0),
            (0, 1.0, 0),
        ];

        for (write_buffer_size, ratio, expected) in cases {
            let limit = compute_mutable_limit(write_buffer_size, ratio);
            assert_eq!(expected, limit);
        }
    }

    #[should_panic]
    #[test]
    fn test_compute_mutable_limit_panic() {
        compute_mutable_limit(80, 1.1);
        compute_mutable_limit(80, -0.1);
    }
}
