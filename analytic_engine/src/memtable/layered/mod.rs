// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MemTable based on skiplist

pub mod factory;
pub mod iter;

use std::{
    mem,
    ops::{Bound, Deref},
    sync::{
        atomic::{self, AtomicU64, AtomicUsize},
        RwLock,
    },
};

use arena::CollectorRef;
use bytes_ext::Bytes;
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatchWithKey, row::Row, schema::Schema,
    time::TimeRange, SequenceNumber,
};
use generic_error::BoxError;
use skiplist::{BytewiseComparator, KeyComparator};
use snafu::{OptionExt, ResultExt};

use crate::memtable::{
    factory::{FactoryRef, Options},
    key::KeySequence,
    layered::iter::ColumnarIterImpl,
    ColumnarIterPtr, Internal, InternalNoCause, MemTable, MemTableRef, Metrics as MemtableMetrics,
    PutContext, Result, ScanContext, ScanRequest,
};

#[derive(Default, Debug)]
struct Metrics {
    row_raw_size: AtomicUsize,
    row_encoded_size: AtomicUsize,
    row_count: AtomicUsize,
}

/// MemTable implementation based on skiplist
pub(crate) struct LayeredMemTable {
    /// Schema of this memtable, is immutable.
    schema: Schema,

    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,

    inner: RwLock<Inner>,

    mutable_switch_threshold: usize,
}

impl LayeredMemTable {
    pub fn new(
        opts: &Options,
        inner_memtable_factory: FactoryRef,
        mutable_switch_threshold: usize,
    ) -> Result<Self> {
        let inner = Inner::new(inner_memtable_factory, opts)?;

        Ok(Self {
            schema: opts.schema.clone(),
            last_sequence: AtomicU64::new(opts.creation_sequence),
            inner: RwLock::new(inner),
            mutable_switch_threshold,
        })
    }
}

impl MemTable for LayeredMemTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        self.inner.read().unwrap().min_key()
    }

    fn max_key(&self) -> Option<Bytes> {
        self.inner.read().unwrap().max_key()
    }

    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        let memory_usage = {
            let inner = self.inner.read().unwrap();
            inner.put(ctx, sequence, row, schema)?;
            inner.approximate_memory_usage()
        };

        if memory_usage > self.mutable_switch_threshold {
            let inner = &mut *self.inner.write().unwrap();
            inner.switch_mutable_segment(self.schema.clone())?;
        }

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        let inner = self.inner.read().unwrap();
        inner.scan(ctx, request)
    }

    fn approximate_memory_usage(&self) -> usize {
        self.inner.read().unwrap().approximate_memory_usage()
    }

    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()> {
        self.last_sequence
            .store(sequence, atomic::Ordering::Relaxed);

        Ok(())
    }

    fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(atomic::Ordering::Relaxed)
    }

    fn time_range(&self) -> Option<TimeRange> {
        let inner = self.inner.read().unwrap();
        inner.time_range()
    }

    fn metrics(&self) -> MemtableMetrics {
        // FIXME: stats and return metrics
        MemtableMetrics::default()
    }
}

/// The inner data structure of [`LayeredMemTable`].
struct Inner {
    mutable_segment_builder: MutableSegmentBuilder,
    mutable_segment: MutableSegment,
    immutable_segments: Vec<ImmutableSegment>,
}

impl Inner {
    fn new(memtable_factory: FactoryRef, opts: &Options) -> Result<Self> {
        let builder_opts = MutableBuilderOptions {
            schema: opts.schema.clone(),
            arena_block_size: opts.arena_block_size,
            collector: opts.collector.clone(),
        };

        let mutable_segment_builder = MutableSegmentBuilder {
            memtable_factory,
            opts: builder_opts,
        };

        // Build the first mutable batch.
        let init_mutable_segment = mutable_segment_builder.build()?;

        Ok(Self {
            mutable_segment_builder,
            mutable_segment: init_mutable_segment,
            immutable_segments: vec![],
        })
    }

    /// Scan batches including `mutable` and `immutable`s.
    #[inline]
    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        let iter = ColumnarIterImpl::new(
            ctx,
            request,
            &self.mutable_segment,
            &self.immutable_segments,
        )?;
        Ok(Box::new(iter))
    }

    #[inline]
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        self.mutable_segment.put(ctx, sequence, row, schema)
    }

    fn switch_mutable_segment(&mut self, schema: Schema) -> Result<()> {
        // Build a new mutable segment, and replace current's.
        let new_mutable = self.mutable_segment_builder.build()?;
        let current_mutable = mem::replace(&mut self.mutable_segment, new_mutable);

        // Convert current's to immutable.
        let scan_ctx = ScanContext::default();
        let scan_req = ScanRequest {
            start_user_key: Bound::Unbounded,
            end_user_key: Bound::Unbounded,
            sequence: common_types::MAX_SEQUENCE_NUMBER,
            projected_schema: ProjectedSchema::no_projection(schema),
            need_dedup: false,
            reverse: false,
            metrics_collector: None,
            time_range: TimeRange::min_to_max(),
        };

        // TODO: do such conversion during the first read.
        let immutable_batches = current_mutable
            .scan(scan_ctx, scan_req)?
            .collect::<Result<Vec<_>>>()?;
        let time_range = current_mutable.time_range().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let max_key = current_mutable.max_key().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let min_key = current_mutable.min_key().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let immutable = ImmutableSegment::new(immutable_batches, time_range, min_key, max_key);

        self.immutable_segments.push(immutable);

        Ok(())
    }

    pub fn min_key(&self) -> Option<Bytes> {
        let comparator = BytewiseComparator;

        let mutable_min_key = self.mutable_segment.min_key();
        let immutable_min_key = if self.immutable_segments.is_empty() {
            return mutable_min_key;
        } else {
            let mut imm_iter = self.immutable_segments.iter();
            // At least one immutable memtable must exist.
            let mut min_key = imm_iter.next().unwrap().min_key();
            for imm in imm_iter {
                if comparator.compare_key(&min_key, &imm.min_key).is_gt() {
                    min_key = imm.min_key();
                }
            }

            min_key
        };

        let min_key = match mutable_min_key {
            None => immutable_min_key,
            Some(mutable_min_key) => {
                if comparator
                    .compare_key(&mutable_min_key, &immutable_min_key)
                    .is_le()
                {
                    mutable_min_key
                } else {
                    immutable_min_key
                }
            }
        };

        Some(min_key)
    }

    pub fn max_key(&self) -> Option<Bytes> {
        let comparator = BytewiseComparator;

        let mutable_max_key = self.mutable_segment.max_key();
        let immutable_max_key = if self.immutable_segments.is_empty() {
            return mutable_max_key;
        } else {
            let mut imm_iter = self.immutable_segments.iter();
            // At least one immutable memtable must exist.
            let mut max_key = imm_iter.next().unwrap().max_key();
            for imm in imm_iter {
                if comparator.compare_key(&max_key, &imm.max_key).is_lt() {
                    max_key = imm.max_key();
                }
            }

            max_key
        };

        let max_key = match mutable_max_key {
            None => immutable_max_key,
            Some(mutable_max_key) => {
                if comparator
                    .compare_key(&mutable_max_key, &immutable_max_key)
                    .is_gt()
                {
                    mutable_max_key
                } else {
                    immutable_max_key
                }
            }
        };
        Some(max_key)
    }

    pub fn time_range(&self) -> Option<TimeRange> {
        let mutable_time_range = self.mutable_segment.time_range();
        let immutable_time_range = if self.immutable_segments.is_empty() {
            return mutable_time_range;
        } else {
            let mut imm_iter = self.immutable_segments.iter();
            let mut time_range = imm_iter.next().unwrap().time_range();
            for imm in imm_iter {
                time_range = time_range.union(imm.time_range());
            }

            time_range
        };

        match mutable_time_range {
            None => Some(immutable_time_range),
            Some(mutable_time_range) => Some(mutable_time_range.union(immutable_time_range)),
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        let mutable_mem_usage = self.mutable_segment.approximate_memory_usage();

        let immutable_mem_usage = self
            .immutable_segments
            .iter()
            .map(|imm| imm.approximate_memory_usage())
            .sum::<usize>();

        mutable_mem_usage + immutable_mem_usage
    }
}

/// Mutable segment for storing the newly written rows.
///
/// It will be converted to [`ImmutableSegment`] when some requirements are met.
pub(crate) struct MutableSegment(MemTableRef);

impl Deref for MutableSegment {
    type Target = MemTableRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Builder for [`MutableSegment`]
struct MutableSegmentBuilder {
    memtable_factory: FactoryRef,
    opts: MutableBuilderOptions,
}

impl MutableSegmentBuilder {
    fn new(memtable_factory: FactoryRef, opts: MutableBuilderOptions) -> Self {
        Self {
            memtable_factory,
            opts,
        }
    }

    fn build(&self) -> Result<MutableSegment> {
        let memtable_opts = Options {
            schema: self.opts.schema.clone(),
            arena_block_size: self.opts.arena_block_size,
            // `creation_sequence` is meaningless in inner memtable, just set it to min.
            creation_sequence: SequenceNumber::MIN,
            collector: self.opts.collector.clone(),
        };

        let memtable = self
            .memtable_factory
            .create_memtable(memtable_opts)
            .box_err()
            .context(Internal {
                msg: "failed to build mutable segment",
            })?;

        Ok(MutableSegment(memtable))
    }
}

struct MutableBuilderOptions {
    pub schema: Schema,

    /// Block size of arena in bytes.
    pub arena_block_size: u32,

    /// Memory usage collector
    pub collector: CollectorRef,
}

/// Immutable segment for storing the read-only record batches.
pub(crate) struct ImmutableSegment {
    /// The record batch converted from raw rows in the [`MutableSegment`]
    record_batches: Vec<RecordBatchWithKey>,

    /// The time range of the record batches
    time_range: TimeRange,

    /// The min key of the record batches
    min_key: Bytes,

    /// The max key of the record batches
    max_key: Bytes,

    /// The memory usage of the record batches
    approximate_memory_size: usize,
}

impl ImmutableSegment {
    fn new(
        record_batches: Vec<RecordBatchWithKey>,
        time_range: TimeRange,
        min_key: Bytes,
        max_key: Bytes,
    ) -> Self {
        let approximate_memory_size = record_batches
            .iter()
            .map(|batch| batch.as_arrow_record_batch().get_array_memory_size())
            .sum();

        Self {
            record_batches,
            time_range,
            min_key,
            max_key,
            approximate_memory_size,
        }
    }

    #[inline]
    pub fn time_range(&self) -> TimeRange {
        self.time_range
    }

    #[inline]
    pub fn min_key(&self) -> Bytes {
        self.min_key.clone()
    }

    #[inline]
    pub fn max_key(&self) -> Bytes {
        self.max_key.clone()
    }

    #[inline]
    pub fn record_batches(&self) -> &[RecordBatchWithKey] {
        &self.record_batches
    }

    #[inline]
    pub fn approximate_memory_usage(&self) -> usize {
        self.approximate_memory_size
    }
}
